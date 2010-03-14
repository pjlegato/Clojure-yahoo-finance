;; clojure-yahoo-finance: Clojure interface to Yahoo! Finance historic stock data.
;;
;; Copyright (C) 2010 Paul Legato. All rights reserved.
;; Licensed under the BSD-new license: see the file LICENSE for details.
;;
;; This code is not endorsed by or connected with Yahoo in any way.
;;
(ns clojure-yahoo-finance.core
  (:import (org.apache.http 
            HttpResponse
            HttpVersion
            client.HttpClient
            client.methods.HttpGet
            conn.scheme.PlainSocketFactory
            conn.scheme.Scheme
            conn.params.ConnManagerParams
            conn.params.ConnPerRouteBean
            conn.scheme.SchemeRegistry
            impl.client.DefaultHttpClient
            impl.conn.tsccm.ThreadSafeClientConnManager
            params.BasicHttpParams
            params.HttpConnectionParams
            params.HttpProtocolParams
            protocol.BasicHttpContext
            util.EntityUtils
            )
           (java.util.concurrent Executors)
           (org.joda.time LocalDate)
           ))

(defn- yahoo-request-thread
  "Internal function used as the thread to retrieve historic data for symbol from startDate to endDate via httpClient.

   The value will be either a map with the symbol name and the raw CSV data from Yahoo, or an HTTP status code (e.g. 404 means the symbol was not found.)

   Dates can be of any class that Joda-Time understands (including a 'YYYY-MM-DD' String.)
"
  [httpClient symbol startDate endDate]
   (let [
         start (LocalDate. startDate)
         end (LocalDate. endDate)

         httpGet (new HttpGet 
                      (str "http://itable.finance.yahoo.com" 
                           "/table.csv?s=" symbol "&g=d" +
                           "&a=" (- (.getMonthOfYear start) 1)
                           "&b=" (.getDayOfMonth start)
                           "&c=" (.getYear start)
                           "&d=" (- (.getMonthOfYear end) 1)
                           "&e=" (.getDayOfMonth end)
                           "&f=" (.getYear end)))

         ]
     (try
      (let [httpResponse  (. httpClient execute httpGet (new BasicHttpContext))
            entity (. httpResponse getEntity)
            data (if (= 200 (.. httpResponse getStatusLine getStatusCode))
                   (. EntityUtils toString entity)
                   404)]
        (.consumeContent entity) ;; required to release the connection back to the pool? Docs are unclear on whether EntityUtils does this itself.
        {symbol data}
        )
      (catch Exception e
        (.abort httpGet)
        (throw e)))))



(defn blocking-query
  "Returns historic data between startDate and endDate for the seq of symbols.

Options:
  :maxConnections - default 200.
  :threadPoolSize - default 300. Should be larger than maxConnections
                    for optimal efficiency. Set to 0 for an unbounded thread pool - be
                    advised that doing so may exhaust available memory if you're requesting a lot of symbols
                    relative to the amount of free memory and thread-creation overhead on your system.

The optimal values for :maxConnections and :threadPoolSize depend on
how much free RAM is available to the Clojure process, how much memory
overhead is involved in creating a new thread (JVM/OS dependent), and
the latency characteristics of the network link between the user and
Yahoo. The defaults should be reasonable for most modern desktop
computers and broadband network environments, but those with access to
particularly fast network pipes and with lots of RAM to spare may be
able to get better performance by tweaking them. Note that you have to
give <code>java</code> the <code>-Xmx</code> flag at startup to
actually make more free RAM available to the JVM.

"
  [startDate endDate symbols & options]
  (let [opts (when options (apply assoc {} options))

        params (new BasicHttpParams)
        schemeRegistry (new SchemeRegistry)

        results {}

        threadPoolSize (or (:threadPoolSize opts) 300)
        threadPool (if (= threadPoolSize 0) 
                     (Executors/newCachedThreadPool)
                     (Executors/newFixedThreadPool threadPoolSize))
        ]

    ;; Wow, this is ugly. Now I remember why I stopped using Java.
    (. HttpProtocolParams setVersion params HttpVersion/HTTP_1_1)
    (. HttpProtocolParams setUserAgent params "Clojure-Yahoo-Finance")

    (. HttpConnectionParams setTcpNoDelay params true)

    ;; If these are turned too low, then you will get timeouts when running very large queries, because 
    ;; all threads are started at once and each will block until it is allocated an HTTP connection from 
    ;; the pool.
    (. HttpConnectionParams setConnectionTimeout params 100000)
    (. ConnManagerParams setTimeout params 100000)


    ;; These methods are marked deprecated in 4.1-alpha1, but their replacements don't work.
    (. ConnManagerParams setMaxConnectionsPerRoute params (new ConnPerRouteBean (or (:maxConnections opts) 200)))
    (. ConnManagerParams setMaxTotalConnections params (or (:maxConnections opts) 200))
    
    (. schemeRegistry register (new Scheme "http" (. PlainSocketFactory getSocketFactory) 80))
    
    (let [connectionManager (new ThreadSafeClientConnManager params schemeRegistry)
          httpClient (new DefaultHttpClient connectionManager params)
          ]

      ;; Broken in 4.1-alpha1 - need to use the deprecated method above instead
      ;;
      ;;(doto connectionManager
      ;; (.setMaxTotalConnections (or (:maxConnections opts) 200)))
      ;;        (.setDefaultMaxPerRoute (or (:maxConnections opts) 200))) 

      (try
       (let [tasks (map (fn [sym]
                          (fn [] (yahoo-request-thread httpClient sym startDate endDate)))
                        symbols
                        )
             futures (map (fn [x] (.get x)) (.invokeAll threadPool tasks))]
         (reduce conj futures))
         
         
         (finally
          (.. httpClient getConnectionManager shutdown)
          (.shutdown threadPool)))
       ))))
    