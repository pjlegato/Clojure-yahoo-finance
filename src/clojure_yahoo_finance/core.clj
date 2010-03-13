(ns yahoofinance.core
  (:import (org.apache.http 
            HttpEntity
            HttpResponse
            HttpVersion
            client.HttpClient
            client.methods.HttpGet
            conn.ClientConnectionManager
            conn.scheme.PlainSocketFactory
            conn.scheme.Scheme
            conn.scheme.SchemeRegistry
            impl.client.DefaultHttpClient
            impl.conn.tsccm.ThreadSafeClientConnManager
            params.BasicHttpParams
            params.HttpParams
            params.HttpConnectionParams
            params.HttpProtocolParams
            protocol.HttpContext
            protocol.BasicHttpContext
            util.EntityUtils
            )
           (java.util.concurrent Executors)
           (org.joda.time LocalDate)
           ))

(defn- yahoo-request-thread
  "Internal function used as the thread to retrieve historic data for symbol from startDate to endDate via httpClient.
   results is a ref to a hash used to hold the results. A new key will be added as (symbol startDate endDate).
   Dates can be of any class that Joda-Time understands (including a 'YYYY-MM-DD' String.)
"
  [results httpClient symbol startDate endDate]
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
      (let [httpResponse  (. httpClient execute httpGet (new BasicHttpContext))]
        (dosync (commute results conj 
                         {symbol
                          (. EntityUtils toString (. httpResponse getEntity))
                          })))

      (catch Exception e
        (.abort httpGet)
        (throw e)))))



(defn blocking-query
  "Returns historic data between startDate and endDate for the seq of symbols.

Options:
  :maxConnectionsPerRoute - default 20
  :maxTotalConnections - default 20
  :threadPoolSize - default 50. Set to 0 for an unbounded thread pool - be advised that doing so may exhaust available memory.

"
  [startDate endDate symbols & options]
  (let [opts (when options (apply assoc {} options))

        params (new BasicHttpParams)
        schemeRegistry (new SchemeRegistry)

        results (ref (hash-map))

        threadPoolSize (or (:threadPoolSize opts) 50)
        threadPool (if (= threadPoolSize 0) 
                     (Executors/newCachedThreadPool)
                     (Executors/newFixedThreadPool threadPoolSize))
        ]

    ;; Wow, this is ugly. Now I remember why I stopped using Java.
    (. HttpProtocolParams setVersion params HttpVersion/HTTP_1_1)
    (. HttpProtocolParams setUserAgent params "Clojure-Yahoo-Finance")

    (. HttpConnectionParams setTcpNoDelay params true)
    (. HttpConnectionParams setStaleCheckingEnabled params false)

    (. schemeRegistry register (new Scheme "http" (. PlainSocketFactory getSocketFactory) 80))
    
    (let [connectionManager (new ThreadSafeClientConnManager params schemeRegistry)
          httpClient (new DefaultHttpClient connectionManager params)
          ]
      (doto connectionManager
        (.setDefaultMaxPerRoute (or (:maxConnectionsPerRoute opts) 20))
        (.setMaxTotalConnections (or (:maxTotalConnections opts) 20)))

      (let [tasks (map (fn [sym]
                         (fn [] (yahoo-request-thread results httpClient sym startDate endDate)))
                       symbols
                       )]
        (doseq [future (.invokeAll threadPool tasks)]
          (.get future))
        (.shutdown threadPool)
        (.. httpClient getConnectionManager shutdown)
        @results
    ))))
    