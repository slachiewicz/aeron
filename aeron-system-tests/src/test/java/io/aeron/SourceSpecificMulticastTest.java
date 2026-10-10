/*
 * Copyright 2014-2026 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron;

import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@ExtendWith(InterruptingTestCallback.class)
class SourceSpecificMulticastTest
{
    private static final int STREAM_ID = 1001;
    private static final String GROUP = "232.1.1.1:20200";
    private static final String SSM_URI_A = "aeron:udp?endpoint=" + GROUP + "|control=127.0.0.1:20201";
    private static final String SSM_URI_B = "aeron:udp?endpoint=" + GROUP + "|control=127.0.0.2:20202";

    @RegisterExtension
    final SystemTestWatcher watcher = new SystemTestWatcher();

    private final MediaDriver.Context context = new MediaDriver.Context()
        .publicationTermBufferLength(LogBufferDescriptor.TERM_MIN_LENGTH)
        .threadingMode(ThreadingMode.SHARED);
    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[64]);
    private TestMediaDriver driver;
    private Aeron aeron;

    @BeforeEach
    void setUp()
    {
        assumeFalse(TestMediaDriver.shouldRunCMediaDriver() && OS.current() == OS.WINDOWS);
        driver = TestMediaDriver.launch(context, watcher);
        watcher.dataCollector().add(driver.context().aeronDirectory());
        aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
    }

    @AfterEach
    void tearDown()
    {
        CloseHelper.closeAll(aeron, driver);
    }

    @Test
    @InterruptAfter(10)
    void shouldConnectAndExchangeMessages()
    {
        try (Publication pub = aeron.addPublication(SSM_URI_A, STREAM_ID);
            Subscription sub = aeron.addSubscription(SSM_URI_A, STREAM_ID))
        {
            Tests.awaitConnected(pub);
            Tests.awaitConnected(sub);

            assertThat(sub.imageAtIndex(0).sourceIdentity(), startsWith("127.0.0.1:"));
            sendAndReceive(pub, sub);
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldOnlyReceiveFromTheSubscribedSource() throws Exception
    {
        assumeTrue(
            null != NetworkInterface.getByInetAddress(InetAddress.getByName("127.0.0.2")),
            "127.0.0.2 is not a local address");

        final List<String> imagesA = new ArrayList<>();
        final List<String> imagesB = new ArrayList<>();

        try (Publication pubA = aeron.addPublication(SSM_URI_A, STREAM_ID);
            Publication pubB = aeron.addPublication(SSM_URI_B, STREAM_ID);
            Subscription subA = aeron.addSubscription(
                SSM_URI_A, STREAM_ID, (image) -> imagesA.add(image.sourceIdentity()), (image) -> {});
            Subscription subB = aeron.addSubscription(
                SSM_URI_B, STREAM_ID, (image) -> imagesB.add(image.sourceIdentity()), (image) -> {}))
        {
            Tests.awaitConnected(pubA);
            Tests.awaitConnected(pubB);
            Tests.awaitConnected(subA);
            Tests.awaitConnected(subB);

            sendAndReceive(pubA, subA);
            sendAndReceive(pubB, subB);

            assertEquals(1, subA.imageCount());
            assertEquals(1, subB.imageCount());
            assertEquals(pubA.sessionId(), subA.imageAtIndex(0).sessionId());
            assertEquals(pubB.sessionId(), subB.imageAtIndex(0).sessionId());
            assertThat(imagesA.get(0), startsWith("127.0.0.1:"));
            assertThat(imagesB.get(0), startsWith("127.0.0.2:"));
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldCoexistWithAnySourceMulticastOnTheSameGroup()
    {
        final String anySourceUri = "aeron:udp?endpoint=" + GROUP + "|interface=127.0.0.1";

        try (Publication ssmPub = aeron.addPublication(SSM_URI_A, STREAM_ID);
            Publication asmPub = aeron.addPublication(anySourceUri, STREAM_ID + 1);
            Subscription ssmSub = aeron.addSubscription(SSM_URI_A, STREAM_ID);
            Subscription asmSub = aeron.addSubscription(anySourceUri, STREAM_ID + 1))
        {
            Tests.awaitConnected(ssmPub);
            Tests.awaitConnected(asmPub);
            Tests.awaitConnected(ssmSub);
            Tests.awaitConnected(asmSub);

            sendAndReceive(ssmPub, ssmSub);
            sendAndReceive(asmPub, asmSub);
        }
    }

    private void sendAndReceive(final Publication pub, final Subscription sub)
    {
        buffer.putInt(0, pub.sessionId());
        while (pub.offer(buffer, 0, Integer.BYTES) < 0)
        {
            Tests.yield();
        }

        final int[] received = new int[1];
        while (0 == sub.poll((buf, offset, length, header) -> received[0] = buf.getInt(offset), 1))
        {
            Tests.yield();
        }

        assertEquals(pub.sessionId(), received[0]);
    }
}
