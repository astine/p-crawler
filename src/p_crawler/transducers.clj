(ns p-crawler.transducers)

(defn- reduced?
  "Returns true if x is the result of a call to reduced"
  {:inline (fn [x] `(clojure.lang.RT/isReduced ~x ))
   :inline-arities #{1}
   :added "1.5"}
  ([x] (clojure.lang.RT/isReduced x)))

(defn- unreduced
  "If x is reduced?, returns (deref x), else returns x"
  {:added "1.7"}
  [x]
  (if (reduced? x) (deref x) x))

(defn ^:private preserving-reduced
  [rf]
  #(let [ret (rf %1 %2)]
     (if (reduced? ret)
       (reduced ret)
       ret)))

(defn catting
  "A transducer which concatenates the contents of each input, which must be a
collection, into the reduction."
  {:added "1.7"}
  [rf]
  (let [rrf (preserving-reduced rf)]
    (fn
      ([] (rf))
      ([result] (rf result))
      ([result input]
         (reduce rrf result input)))))

(defn partitioning-all [^long n]
  (fn [rf]
    (let [a (java.util.ArrayList. n)]
      (fn
        ([] (rf))
        ([result]
           (let [result (if (.isEmpty a)
                          result
                          (let [v (vec (.toArray a))]
                            ;;clear first!
                            (.clear a)
                            (unreduced (rf result v))))]
             (rf result)))
        ([result input]
           (.add a input)
           (if (= n (.size a))
             (let [v (vec (.toArray a))]
               (.clear a)
               (rf result v))
             result))))))

(defn mapping [f]
  (fn [rf]
    (fn
      ([] (rf))
      ([result] (rf result))
      ([result input]
         (rf result (f input)))
      ([result input & inputs]
         (rf result (apply f input inputs))))))

(defn mapcatting [f]
  (comp (mapping f) catting))
