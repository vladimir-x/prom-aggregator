package ru.dude.aggregator;

import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import javax.servlet.*;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class Aggregator {

    private static int queueSize = 100000;
    private static int queueDelay = 1000;
    private static int listenPort = 8099;


    private static ConcurrentHashMap<String, Metric> store = new ConcurrentHashMap<>();

    private static ArrayBlockingQueue<Metric> queue = new ArrayBlockingQueue<>(queueSize);

    // для пришедших метрик
    private static ScheduledExecutorService sheduleExecutor = Executors.newScheduledThreadPool(1);

    // для систменых метрик
    private static ScheduledExecutorService systemExecutor = Executors.newScheduledThreadPool(1);

    private static String rrrrrrr = "";

    /// Внутренние метрики
    private static Metric packSizeMetric = new Metric("aggregate_pack_size", Metric.Type.GAUGE, new BigDecimal(0));
    private static Metric draininigTimeMetric = new Metric("aggregate_draining_ms", Metric.Type.GAUGE, new BigDecimal(0));
    private static Metric simultaneouslyMetric = new Metric("aggregate_simultaneously_count", Metric.Type.GAUGE, new BigDecimal(0));
    private static Metric postPerSecond = new Metric("aggregate_post_per_second_count", Metric.Type.GAUGE, new BigDecimal(0));
    //private static Metric cpuUsageMetric = new Metric("aggregate_cpu_usage", Metric.Type.GAUGE,new BigDecimal(0));

    private static volatile int packCount;
    private static volatile int packSizeAll;

    private static AtomicLong totalPost = new AtomicLong();
    private static AtomicLong prevTotalPost = new AtomicLong();

    private static AtomicLong simultaneouslyPost = new AtomicLong();
    private static AtomicLong draininigTime = new AtomicLong();




    public static class Metric {

        public enum Type {COUNTER, GAUGE}

        String name;
        Type type;

        BigDecimal value;

        static Type parseType(String x) {
            if (x.length() > 0 && (x.charAt(0) == 'c' || x.charAt(0) == 'C')) {
                return Type.COUNTER;
            }
            // по умолчанию
            return Type.GAUGE;
        }

        public Metric(String name, Type type, BigDecimal value) {
            this.name = name;
            this.type = type;
            this.value = value;
        }
    }


    public static class SystemMetricCalc{

        Metric metric;

        AtomicLong prevPost;

        public SystemMetricCalc(Metric metric) {
            this.metric = metric;
        }



    }


    private static class Listener extends HttpServlet {

        @Override
        protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
            Request request = ((Request) req);
            resp.setContentType("text; charset=UTF-8");
            resp.setCharacterEncoding("UTF8");
            if (request.getServletPath().equals("/metrics")) {
                for (Metric metric : store.values()) {
                    resp.getWriter().print(metric.name);
                    resp.getWriter().print(" ");
                    resp.getWriter().print(metric.value);
                    resp.getWriter().print("\n");
                }

                clearSystemMertic();
            } else {
                super.doGet(req, resp);
            }
        }

        @Override
        protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
            totalPost.incrementAndGet();
            simultaneouslyPost.incrementAndGet();

            Request request = ((Request) req);
            BufferedReader br = new BufferedReader(new InputStreamReader(request.getInputStream(), Charset.forName("UTF8")));

            //if (simultaneouslyPost.get()>1) {
                //simultaneouslyMetric.value = new BigDecimal(simultaneouslyPost.get());
                //rrrrrrr = "simultaneouslyPost.get() = " + simultaneouslyPost.get();
                //System.out.println("simultaneouslyPost.get() = " + simultaneouslyPost.get());
            //}

            String line = "";
            while ((line = br.readLine()) != null) {
                try {

                    line = line.trim();
                    if (line.length() > 0 && !line.startsWith("#")) {

                        String[] split = line.split("\\|");

                        if (split.length > 1) {
                            String name = split[0].trim();
                            BigDecimal value = new BigDecimal(split[1].trim());
                            Metric.Type type = split.length > 2 ? Metric.parseType(split[2].trim()) : Metric.Type.GAUGE;

                            queue.add(new Metric(name, type, value));
                        }
                    }
                } catch (Exception ex) {
                    ex.printStackTrace();
                }

            }

            simultaneouslyPost.decrementAndGet();
        }
    }

    private static void fillSystemMertic() {
        packSizeMetric.value = new BigDecimal(packCount > 0 ? packSizeAll / packCount : 0);
        draininigTimeMetric.value = new BigDecimal(packCount > 0 ? draininigTime.get() / packCount : 0);

        simultaneouslyMetric.value = new BigDecimal(simultaneouslyPost.get());
        postPerSecond.value = new BigDecimal(totalPost.get()-prevTotalPost.get());

    }

    private static void clearSystemMertic() {
        packSizeAll = 0;
        packCount = 0;
        draininigTime.set(0);

    }

    private static void fillStore(List<Metric> pack) {
        for (Metric m : pack) {
            Metric stored = store.get(m.name);

            if (stored == null) {
                store.put(m.name, m);
            } else {
                if (stored.type == Metric.Type.COUNTER) {
                    stored.value = stored.value.add(m.value);
                } else {
                    stored.value = m.value;
                }
            }
        }

        packCount += 1;
        packSizeAll += pack.size();
    }

    public static void main(String[] args) throws Exception {

        Server server = null;
        try {
            server = new Server();
            ServerConnector connector = new ServerConnector(server);
            connector.setPort(listenPort);
            server.setConnectors(new Connector[]{connector});

            ServletContextHandler handler = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);

            handler.addServlet(new ServletHolder(new Listener()), "/");

            server.setHandler(handler);

            store.put(packSizeMetric.name, packSizeMetric);
            store.put(simultaneouslyMetric.name, simultaneouslyMetric);
            store.put(draininigTimeMetric.name, draininigTimeMetric);
            store.put(postPerSecond.name, postPerSecond);



            sheduleExecutor.scheduleWithFixedDelay(new Runnable() {
                @Override
                public void run() {

                    long st = System.currentTimeMillis();
                    ArrayBlockingQueue<Metric> buff = queue;
                    queue = new ArrayBlockingQueue<Metric>(queueSize);

                    ArrayList<Metric> pack = new ArrayList<>();
                    buff.drainTo(pack);

                    fillStore(pack);

                    draininigTime.addAndGet(System.currentTimeMillis() - st);
                    fillSystemMertic();


                }
            }, queueDelay, queueDelay, TimeUnit.MILLISECONDS);


            systemExecutor.scheduleWithFixedDelay(new Runnable() {
                @Override
                public void run() {
                    fillSystemMertic();

                    prevTotalPost.set(totalPost.get());
                }
            }, 1, 1, TimeUnit.SECONDS);


            server.start();
            server.join();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            server.stop();
        }
    }
}
