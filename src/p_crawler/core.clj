(ns p-crawler.core
  "This is the primary ."
  (:gen-class)
  (:require [clojure.core.async :refer [thread go onto-chan chan <!! <!]]
            [monger.core :as mg]
            [monger.collection :as mc]
            [monger.joda-time :refer :all]
            [net.cgrand.enlive-html :as html]
            [clojurewerkz.urly.core :as url]
            [taoensso.timbre :as logger]
            [clojure.string :refer [split]]
            [clj-http.client :as http]
            [clj-robots.core :as robots]
            [clj-time.core :as time]
            [clj-time.coerce :as coerce-time])
  (:import [java.net URL UnknownHostException]
           [org.bson.types ObjectId]))

(logger/refer-timbre)

(def db (mg/get-db (mg/connect) "p-crawler"))

(def url-chan (chan 10))

(def url-queue (agent {:queue []
                       :members #{}}))

(def domains (atom {}))

(def robots-expire-duration (time/days 1))

(def default-crawl-delay (time/seconds 30))

(def connection-manager (clj-http.conn-mgr/make-socks-proxied-conn-manager "localhost" 8080))

(def connection-defaults {;:connection-manager connection-manager
                          :conn-timeout 1000
                          :socket-timeout 1000
                          :throw-exceptions false
                          :follow-redirects false})

(defn update-domain! [domain key value]
  (let [return (get-in (swap! domains assoc-in [domain key] value)
                       [domain key])]
    (go (mc/update-by-id db "domains" domain
                         (array-map :$set (array-map :domain domain key value))
                         {:upsert true}))
    return))

(defn get-domain-value [domain key]
  (or (get-in @domains [domain key])
      (key (mc/find-map-by-id db "domains" domain))))

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
  `(let [old-state# (:state @threadd)]
     (swap! threadd assoc :state ~state)
     (let [output# (do ~@body)]
       (swap! threadd assoc :state old-state#)
       output#)))

(defn extract-domain-from-url [url]
  (set-state [:extract-domain-from-url url]
             (when url
               (let [[[match protocol domain path]]
                     (re-seq #"^(\w*://)?([^:_/]+\.\w+)([:/].*)?" url)
                     tld (when domain (last (split domain #"\.")))]
                 (when (#{"ac" "ad" "ae" "aero" "af" "ag" "ai" "al" "am" "an" "ao" "aq" "ar" "arpa" "as" "asia" "at" "au" "aw" "ax" "az" "ba" "bb" "bd" "be" "bf" "bg" "bh" "bi" "biz" "bj" "bm" "bn" "bo" "br" "bs" "bt" "bv" "bw" "by" "bz" "ca" "cat" "cc" "cd" "cf" "cg" "ch" "ci" "ck" "cl" "cm" "cn" "co" "com" "coop" "cr" "cu" "cv" "cw" "cx" "cy" "cz" "de" "dj" "dk" "dm" "do" "dz" "ec" "edu" "ee" "eg" "er" "es" "et" "eu" "fi" "fj" "fk" "fm" "fo" "fr" "ga" "gb" "gd" "ge" "gf" "gg" "gh" "gi" "gl" "gm" "gn" "gov" "gp" "gq" "gr" "gs" "gt" "gu" "gw" "gy" "hk" "hm" "hn" "hr" "ht" "hu" "id" "ie" "il" "im" "in" "info" "int" "io" "iq" "ir" "is" "it" "je" "jm" "jo" "jobs" "jp" "ke" "kg" "kh" "ki" "km" "kn" "kp" "kr" "kw" "ky" "kz" "la" "lb" "lc" "li" "lk" "lr" "ls" "lt" "lu" "lv" "ly" "ma" "mc" "md" "me" "mg" "mh" "mil" "mk" "ml" "mm" "mn" "mo" "mobi" "mp" "mq" "mr" "ms" "mt" "mu" "museum" "mv" "mw" "mx" "my" "mz" "na" "name" "nc" "ne" "net" "nf" "ng" "ni" "nl" "no" "np" "nr" "nu" "nz" "om" "org" "pa" "pe" "pf" "pg" "ph" "pk" "pl" "pm" "pn" "post" "pr" "pro" "ps" "pt" "pw" "py" "qa" "re" "ro" "rs" "ru" "rw" "sa" "sb" "sc" "sd" "se" "sg" "sh" "si" "sj" "sk" "sl" "sm" "sn" "so" "sr" "st" "su" "sv" "sx" "sy" "sz" "tc" "td" "tel" "tf" "tg" "th" "tj" "tk" "tl" "tm" "tn" "to" "tp" "tr" "travel" "tt" "tv" "tw" "tz" "ua" "ug" "uk" "us" "uy" "uz" "va" "vc" "ve" "vg" "vi" "vn" "vu" "wf" "ws" "xn" "xyz" "xxx" "ye" "yt" "za" "zm" "zw"} tld)
                   domain)))))

(defn fetch-links [domain]
  (set-state :fetch-links
             (info (str "fetching: " domain))
             (try
               (let [{:keys [body] :as response}
                     (http/get (str "http://" domain)
                               connection-defaults)]
                 (swap! threadd assoc :state :parse)
                 (map (comp :href :attrs)
                      (html/select
                       (html/html-resource (java.io.StringReader. body))
                       [:a])))
               (catch Exception e
                 (error e)
                 nil))))

(defn get-remote-domains [domain links]
  (set-state :get-remote-domains
             (set
              (remove #(or (nil? %) (= % domain))
                      (map extract-domain-from-url links)))))

(defn links [domain]
  (set-state :links
             (let [crawl-delay (or (:crawls-delay (robots domain)) default-crawl-delay)
                   links (get-domain-value domain :links)]
               (if (or (not links)
                       (time/before? (get-domain-value domain :last-parse-time)
                                     (time/minus (time/now) crawl-delay)))
                 (update-domain! domain :links
                                 (do
                                   (update-domain! domain :last-parse-time (time/now))
                                   (get-remote-domains domain (fetch-links domain))))
                 links))))

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

(defn save-queue []
  (mc/remove db "url_queue")
  (mc/insert-batch db "url_queue" (map (partial array-map :_id) @url-queue)))
  

(defn process-url [url]
  (set-state :process-url
             (let [links (links url)]
               (when links
                 (enqueue-urls links)))))

(def threadds (repeatedly 8 #(atom {})))

(defn crawl-web [seed]
  (start-queue-transfer seed)
  (doseq [threadd threadds]
    (thread (binding [threadd threadd]
	      (loop [url (<!! url-chan)]
                (when url
                  (swap! threadd (constantly {:time (time/now) :fetching url :count (inc (or (:count @threadd) 0))}))
                  (process-url url)
                  (recur (<!! url-chan))))))))

(logger/set-level! :trace)

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (println "Hello, World!"))
