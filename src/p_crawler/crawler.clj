(ns p-crawler.crawler
  (:require [clojure.core.async :refer [thread go onto-chan chan <!! <! close!]
             :as async]
            [monger.core :as mg]
            [monger.collection :as mc]
            [monger.joda-time :refer :all]
            [clojurewerkz.urly.core :as url]
            [taoensso.timbre :as logger]
            [clojure.string :refer [split]]
            [clj-http.client :as http]
            [clj-robots.core :as robots]
            [clj-time.core :as time]
            [clj-time.coerce :as coerce-time]
            [p-crawler.database :refer :all]
            [p-crawler.page-parser :refer :all])
  (:import [java.net URL UnknownHostException ConnectException]
           [java.io IOException]
           [org.apache.http.conn ConnectTimeoutException]
           [org.bson.types ObjectId]))

(logger/refer-timbre)

(def url-chan (chan 10))

(def url-queue (agent {:queue []
                       :members #{}}))

(def robots-expire-duration (time/days 1))

(def default-crawl-delay (time/seconds 30))

(def connection-manager (clj-http.conn-mgr/make-socks-proxied-conn-manager "localhost" 8080))

(def connection-defaults {;:connection-manager connection-manager
                          :conn-timeout 1000
                          :socket-timeout 1000
                          :throw-exceptions false
                          :follow-redirects false})

(defn update-domain! [domain key value & [synchronously]]
  (update-document! "domains" [domain key] value synchronously))

(defn get-domain-value [domain key]
  (query-document "domains" [domain key]))

(defn fetch-robots [domain]
  (try
    (let [{:keys [body] :as response}
          (http/get (str "http://" domain "/robots.txt")
                    connection-defaults)]
      (robots/parse body))
    (catch Exception e
                                        ;(error e)
      nil)))

(defn robots [domain]
  (let [robots (get-domain-value domain :robots)]
    (if (or (not robots)
            (time/before? (coerce-time/from-long (:modified-time robots))
                          (time/minus (time/now) robots-expire-duration)))
      (update-domain! domain :robots (fetch-robots domain))
      robots)))

(def ^:dynamic threadd nil)

(defmacro set-state [state & body]
  `(if threadd
     (let [old-state# (:state @threadd)]
       (swap! threadd assoc :state ~state)
       (let [output# (do ~@body)]
         (swap! threadd assoc :state old-state#)
         output#))
     (do ~@body)))

(defn extract-domain-from-url [url]
  (set-state [:extract-domain-from-url url]
             (when url
               (let [[[match protocol domain path]]
                     (re-seq #"^(\w*://)?([^:_/]+\.\w+)([:/].*)?" url)
                     tld (when domain (last (split domain #"\.")))]
                 (when (#{"ac" "ad" "ae" "aero" "af" "ag" "ai" "al" "am" "an" "ao" "aq" "ar" "arpa" "as" "asia" "at" "au" "aw" "ax" "az" "ba" "bb" "bd" "be" "bf" "bg" "bh" "bi" "biz" "bj" "bm" "bn" "bo" "br" "bs" "bt" "bv" "bw" "by" "bz" "ca" "cat" "cc" "cd" "cf" "cg" "ch" "ci" "ck" "cl" "cm" "cn" "co" "com" "coop" "cr" "cu" "cv" "cw" "cx" "cy" "cz" "de" "dj" "dk" "dm" "do" "dz" "ec" "edu" "ee" "eg" "er" "es" "et" "eu" "fi" "fj" "fk" "fm" "fo" "fr" "ga" "gb" "gd" "ge" "gf" "gg" "gh" "gi" "gl" "gm" "gn" "gov" "gp" "gq" "gr" "gs" "gt" "gu" "gw" "gy" "hk" "hm" "hn" "hr" "ht" "hu" "id" "ie" "il" "im" "in" "info" "int" "io" "iq" "ir" "is" "it" "je" "jm" "jo" "jobs" "jp" "ke" "kg" "kh" "ki" "km" "kn" "kp" "kr" "kw" "ky" "kz" "la" "lb" "lc" "li" "lk" "lr" "ls" "lt" "lu" "lv" "ly" "ma" "mc" "md" "me" "mg" "mh" "mil" "mk" "ml" "mm" "mn" "mo" "mobi" "mp" "mq" "mr" "ms" "mt" "mu" "museum" "mv" "mw" "mx" "my" "mz" "na" "name" "nc" "ne" "net" "nf" "ng" "ni" "nl" "no" "np" "nr" "nu" "nz" "om" "org" "pa" "pe" "pf" "pg" "ph" "pk" "pl" "pm" "pn" "post" "pr" "pro" "ps" "pt" "pw" "py" "qa" "re" "ro" "rs" "ru" "rw" "sa" "sb" "sc" "sd" "se" "sg" "sh" "si" "sj" "sk" "sl" "sm" "sn" "so" "sr" "st" "su" "sv" "sx" "sy" "sz" "tc" "td" "tel" "tf" "tg" "th" "tj" "tk" "tl" "tm" "tn" "to" "tp" "tr" "travel" "tt" "tv" "tw" "tz" "ua" "ug" "uk" "us" "uy" "uz" "va" "vc" "ve" "vg" "vi" "vn" "vu" "wf" "ws" "xn" "xyz" "xxx" "ye" "yt" "za" "zm" "zw"} tld)
                   domain)))))

(defn fetch-index [domain]
  (set-state :fetch-index
             (info (str "fetching: " domain))
             (try
               (let [{:keys [body] :as response}
                     (http/get (str "http://" domain)
                               connection-defaults)]
                 body)
               (catch UnknownHostException e
                 (update-domain! domain :unable-to-download true)
                 nil)
               (catch ConnectTimeoutException e
                 (update-domain! domain :unable-to-download true)
                 nil)
               (catch ConnectException e
                 (update-domain! domain :unable-to-download true)
                 nil)
               (catch IOException e
                 (update-domain! domain :unable-to-download true)
                 nil)
               (catch Exception e
                 (error e)
                 nil))))

(defn get-remote-domains [domain links]
  (set-state :get-remote-domains
             (set
              (remove #(or (nil? %) (= % domain))
                      (map extract-domain-from-url links)))))

(defmacro update-with-crawl-delay [[domain field] & body]
  `(let [crawl-delay# (or (:crawls-delay (robots ~domain)) default-crawl-delay)
         value# (get-domain-value ~domain ~field)]
     (if (or (not value#)
             (time/before? (get-domain-value ~domain :last-parse-time)
                           (time/minus (time/now) crawl-delay#)))
       (update-domain! ~domain ~field ~@body)
       value#)))
 
(defn body [domain]
  (set-state :body
               (update-with-crawl-delay [domain :body]
                 (do
                   (update-domain! domain :last-parse-time (time/now))
                   (fetch-index domain)))))

(defn links [domain]
  (set-state :links
             (update-with-crawl-delay [domain :links]
               (get-remote-domains domain (extract-links (or (body domain) ""))))))

(defn tokens [domain]
  (set-state :tokens
             (update-with-crawl-delay [domain :tokens]
               (webpage-to-token-bag (or (body domain) "")))))

(defn enqueue-urls [urls]
  (send url-queue
        (fn [{:keys [queue members]} urls]
          {:queue (into queue (remove members urls))
           :members (clojure.set/union members (set urls))})
        urls))

(defn start-queue-transfer [seed]
  (letfn [(pipe-queue [{:keys [queue members]}]
            (let [batch (take 10 queue)]
              (go (<! (onto-chan url-chan batch false))
                  (send url-queue pipe-queue))
              {:queue (vec (drop 10 queue))
               :members (clojure.set/difference members batch)}))]
    (enqueue-urls seed)
    (send url-queue pipe-queue)))

(defn clear-queue []
  (send url-queue (constantly {:queue []
                               :members #{}}))
  (close! url-chan)
  (<!! (async/reduce conj [] url-chan)))

(defn save-queue []
  (mc/insert-batch db "url_queue" (map (partial array-map :_id) @url-queue))
  (close! url-chan)
  (mc/insert-batch db "url_queue" (<!! (async/reduce conj [] url-chan))))

(defn retrieve-queue []
  (let [queue (mc/find-maps db "url_queue")]
    (mc/remove db "url_queue")
    queue))

(defn process-url [url]
  (set-state :process-url
             (let [links (links url)]
               (tokens url)
               (when links
                 (enqueue-urls links)))))

(def threadds (repeatedly 8 #(atom {})))

(defn crawl-web [seed hook]
  (start-queue-transfer seed)
  (doseq [threadd threadds]
    (thread (binding [threadd threadd]
	      (loop [url (<!! url-chan)]
                (when url
                  (swap! threadd (constantly {:time (time/now) :fetching url :count (inc (or (:count @threadd) 0))}))
                  (process-url url)
                  (hook url)
                  (recur (<!! url-chan))))))))

(logger/set-level! :trace)
