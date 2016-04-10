package org.infinispan.tracing;

import com.github.kristofa.brave.Brave;
import com.github.kristofa.brave.ClientTracer;
import com.github.kristofa.brave.ServerSpan;
import com.github.kristofa.brave.ServerTracer;
import com.github.kristofa.brave.SpanCollector;
import com.github.kristofa.brave.SpanCollectorMetricsHandler;
import com.github.kristofa.brave.SpanId;
import com.github.kristofa.brave.http.HttpSpanCollector;
import org.testng.annotations.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

@Test(groups = "unit", testName = "LiveSystemTest")
public class LiveSystemTest {

   SpanCollector collector = HttpSpanCollector.create(
         "http://192.168.99.100:9411/", new LoggingSpanCollectorMetrics());

   public void testChainedServerTraces() {
      Brave brave = new Brave.Builder("zipkin-try-test").spanCollector(collector).build();
      final ClientTracer ct = brave.clientTracer();
      final ServerTracer st = brave.serverTracer();
      final SpanId span = ct.startNewSpan("Try server tracing...");
      ct.setCurrentClientServiceName("zipkin-try-test");
      st.setStateCurrentTrace(span.getTraceId(), span.getSpanId(), null, "zipkin-try-test");
      st.setServerReceived();
      sleep(100);

      final ServerSpan currentServerSpan = brave.serverSpanThreadBinder().getCurrentServerSpan();

      ExecutorService executor = Executors.newCachedThreadPool();
      Future<Object> f = executor.submit(() -> {
         SpanCollector collector1 = HttpSpanCollector.create(
               "http://192.168.99.100:9411/", new LoggingSpanCollectorMetrics());
         Brave brave1 = new Brave.Builder("zipkin-try-remote-test").spanCollector(collector1).build();

         brave1.serverSpanThreadBinder().setCurrentSpan(currentServerSpan);

         SpanId span2 = brave1.clientTracer().startNewSpan("replicated");
         brave1.serverTracer().setStateCurrentTrace(span2.getTraceId(), span2.getSpanId(), currentServerSpan.getSpan().getId(), "zipkin-try-remote-test");
         st.setServerReceived();
         sleep(100);
         st.setServerSend();

         // Wait for flush
         sleep(2000);
         return null;
      });
      futureGet(f);
      executor.shutdown();
      sleep(100);
      st.setServerSend();
      // Wait for flush
      sleep(2000);
   }

   public void testSingleServerTraceWithClientTrace() {
      Brave brave = new Brave.Builder("zipkin-try-test").spanCollector(collector).build();
      final ClientTracer ct = brave.clientTracer();
      final ServerTracer st = brave.serverTracer();
      final SpanId span = ct.startNewSpan("Try server tracing...");
      ct.setCurrentClientServiceName("zipkin-try-test");
      st.setStateCurrentTrace(span.getTraceId(), span.getSpanId(), null, "zipkin-try-test");
      st.setServerReceived();
      sleep(100);

      final ServerSpan currentServerSpan = brave.serverSpanThreadBinder().getCurrentServerSpan();

      ExecutorService executor = Executors.newCachedThreadPool();
      Future<Object> f = executor.submit(() -> {
         SpanCollector collector1 = HttpSpanCollector.create(
               "http://192.168.99.100:9411/", new LoggingSpanCollectorMetrics());
         Brave brave1 = new Brave.Builder("zipkin-try-mysql-test").spanCollector(collector1).build();

         brave1.serverSpanThreadBinder().setCurrentSpan(currentServerSpan);

         brave1.clientTracer().startNewSpan("query");
         //brave1.clientTracer().setCurrentClientServiceName("zipkin-try-test");
         brave1.clientTracer().submitBinaryAnnotation("try.query", "select * from world");
         brave1.clientTracer().setClientSent();
         sleep(100);
         brave1.clientTracer().setClientReceived();
         // Wait for flush
         sleep(2000);
         return null;
      });
      futureGet(f);
      executor.shutdown();
      sleep(100);
      st.setServerSend();
      // Wait for flush
      sleep(2000);
   }

   public void testSingleServerTrace() {
      Brave brave = new Brave.Builder("zipkin-try-test").spanCollector(collector).build();
      ClientTracer ct = brave.clientTracer();
      ServerTracer st = brave.serverTracer();
      SpanId span = ct.startNewSpan("Try server tracing...");
      ct.setCurrentClientServiceName("zipkin-try-test");
      st.setStateCurrentTrace(span.getTraceId(), span.getSpanId(), null, "zipkin-try-test");
      st.setServerReceived();
      sleep(100);
      st.setServerSend();
      // Wait for flush
      sleep(2000);
   }

   public void testSingleClientTrace() {
      Brave brave = new Brave.Builder("zipkin-try-test").spanCollector(collector).build();
      ClientTracer ct = brave.clientTracer();
      ct.startNewSpan("Try tracing...");
      ct.submitAnnotation("Sample client annotation");
      ct.setClientSent();
      sleep(100);
      ct.setClientReceived();
      // Wait for flush
      sleep(2000);
   }

   private void sleep(long time) {
      try {
         Thread.sleep(time);
      } catch (InterruptedException e) {
         throw new RuntimeException(e);
      }
   }

   private void futureGet(Future<Object> f) {
      try {
         f.get(10, TimeUnit.SECONDS);
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   static class LoggingSpanCollectorMetrics implements SpanCollectorMetricsHandler {

      @Override
      public void incrementAcceptedSpans(int quantity) {
         System.out.println("Span accepted: " + quantity);
      }

      @Override
      public void incrementDroppedSpans(int quantity) {
         System.out.println("Span not accepted: " + quantity);
      }
   }
}
