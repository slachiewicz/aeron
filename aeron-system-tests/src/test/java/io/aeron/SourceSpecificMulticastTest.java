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
import org.agrona.Strings;
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
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@ExtendWith(InterruptingTestCallback.class)
class SourceSpecificMulticastTest
{
    private static final int STREAM_ID = 1001;
    private static final String GROUP = "232.1.1.1:20200";
    private static final String SSM_URI_A = "aeron:udp?endpoint=" + GROUP + "|control=127.0.0.1:20201";
    private static final String SSM_URI_B = "aeron:udp?endpoint=" + GROUP + "|control=127.0.0.2:20202";
    private static final String IPV6_SOURCE_ENV_VAR = "AERON_TEST_SSM_IPV6_SOURCE";

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

    @Test
    @InterruptAfter(10)
    void shouldNotConnectToPublicationFromAnotherSource()
    {
        final String otherSourceUri = SSM_URI_A + "|source=127.0.0.2";

        try (Publication pub = aeron.addPublication(SSM_URI_A, STREAM_ID);
            Subscription sub = aeron.addSubscription(otherSourceUri, STREAM_ID))
        {
            final long deadlineNs = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
            while (System.nanoTime() < deadlineNs)
            {
                pub.offer(buffer, 0, Integer.BYTES);
                assertFalse(sub.isConnected());
                Tests.sleep(10);
            }
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldReceiveFromSourceSpecificAndUnicastDestinations()
    {
        final String unicastUri = "aeron:udp?endpoint=127.0.0.1:20210";

        try (Subscription sub = aeron.addSubscription("aeron:udp?control-mode=manual", STREAM_ID);
            Publication ssmPub = aeron.addPublication(SSM_URI_A, STREAM_ID);
            Publication unicastPub = aeron.addPublication(unicastUri, STREAM_ID))
        {
            sub.addDestination(SSM_URI_A);
            sub.addDestination(unicastUri);

            Tests.awaitConnected(ssmPub);
            Tests.awaitConnected(unicastPub);
            while (sub.imageCount() < 2)
            {
                Tests.yield();
            }

            sendAndReceive(ssmPub, sub);
            sendAndReceive(unicastPub, sub);
        }
    }

    @Test
    @InterruptAfter(20)
    void shouldReconnectWhenPublicationIsRecreated()
    {
        try (Subscription sub = aeron.addSubscription(SSM_URI_A, STREAM_ID))
        {
            try (Publication pub = aeron.addPublication(SSM_URI_A, STREAM_ID))
            {
                Tests.awaitConnected(pub);
                Tests.awaitConnected(sub);
                sendAndReceive(pub, sub);
            }

            while (sub.isConnected())
            {
                Tests.yield();
            }

            try (Publication pub = aeron.addPublication(SSM_URI_A, STREAM_ID))
            {
                Tests.awaitConnected(pub);
                Tests.awaitConnected(sub);
                sendAndReceive(pub, sub);
            }
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldConnectAndExchangeMessagesOverIPv6()
    {
        if (!TestMediaDriver.shouldRunCMediaDriver())
        {
            assumeFalse("true".equals(System.getProperty("java.net.preferIPv4Stack")), "IPv4 stack preferred");
            assumeFalse(OS.current() == OS.MAC, "the JDK does not support IPv6 source-specific multicast on macOS");
        }

        final String configuredSource = System.getenv(IPV6_SOURCE_ENV_VAR);
        assumeFalse(
            Strings.isEmpty(configuredSource) && OS.current() == OS.LINUX,
            "Linux does not loop IPv6 multicast back on the loopback interface, set " + IPV6_SOURCE_ENV_VAR +
            " to the IPv6 address of a multicast capable interface");

        final String source = Strings.isEmpty(configuredSource) ? "::1" : configuredSource;
        final String uri = "aeron:udp?endpoint=[ff3e::8000:1]:20220|control=[" + source + "]:20221";

        try (Publication pub = aeron.addPublication(uri, STREAM_ID);
            Subscription sub = aeron.addSubscription(uri, STREAM_ID))
        {
            Tests.awaitConnected(pub);
            Tests.awaitConnected(sub);
            sendAndReceive(pub, sub);
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
