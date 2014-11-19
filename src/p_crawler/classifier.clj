(ns p-crawler.classifier
  (:require [monger.core :as mg]
            [monger.query :as mq]
            [monger.joda-time :refer :all]
            [clojure.string :refer [lower-case]]
            [clojure.set :refer [difference intersection union]]
            [clojure.core.async :refer [go]]
            [p-crawler.database :refer :all]
            [p-crawler.crawler :refer :all])
  (:import [java.net URL]))

(defn- dotted-key
  ([keys]
     (clojure.string/join "." (map name keys)))
  ([k & keys]
     (dotted-key (cons k keys))))

(defn fetch-classified-domains [category matched? count]
  (map :domain
       (mq/with-collection db "domains"
         (mq/find {(dotted-key :manual_class category) (if matched? 1 0)
                   :unable-to-download {:$ne true}})
         (mq/fields [:domain :tokens])
         (mq/sort (array-map (dotted-key :manual_class category) (if matched? -1 1)))
         (mq/limit count))))
  
(defn classified-domain-tokens [category matched? count]
  (map tokens (fetch-classified-domains category matched? count)))

(def classifiers (atom {}))

(defn save-classifier! [{:keys [category] :as classifier}]
  (update-document! "classifiers" [category] classifier))

(defn generate-classifier [category]
  {:category category
   :probabilities {}
   :count 0
   :anti-probabilities {}
   :anti-count 0} )

(defn classifier [category]
  (or (get-document "classifiers" category)
      (update-document! "classifiers" [category]
                        (generate-classifier category))))

(defn update-classifier! [category fn]
  (-> (classifier category)
      fn
      save-classifier!))
  
(defn increment-token-probability [prior prior-count]
  (/ (+ (* prior prior-count) 1)
     (inc prior-count)))

(defn decrement-token-probability [prior prior-count]
  (/ (+ (* prior prior-count) 0)
     (inc prior-count)))

(defn process-document-tokens [{:keys [probabilities anti-probabilities count anti-count] :as classifier} tokens match?]
  (let [ncount (if match? (or count 0) (or anti-count 0))
        old-tokens (set (keys probabilities))
        new-tokens (difference tokens old-tokens)
        incrementing-tokens (intersection tokens old-tokens)
        decrementing-tokens (difference old-tokens tokens)
        inc-probs (as-> (transient (or (if match? probabilities anti-probabilities) {})) inc-probs
                        (reduce #(assoc! %1 %2 (/ 1 (inc ncount)))
                                inc-probs new-tokens)
                        (reduce #(assoc! %1 %2 (increment-token-probability (get %1 %2) ncount))
                                inc-probs incrementing-tokens)
                        (reduce #(assoc! %1 %2 (decrement-token-probability (get %1 %2) ncount))
                                inc-probs decrementing-tokens))
        dec-probs (reduce #(assoc! %1 %2 0)
                          (transient (or (if match? anti-probabilities probabilities) {}))
                          new-tokens)]
    (assoc classifier
     :count (if match? (inc ncount) count)
     :anti-count (if match? anti-count (inc ncount))
     :probabilities (persistent! (if match? inc-probs dec-probs))
     :anti-probabilities (persistent! (if match? dec-probs inc-probs)))))
    
(defn train-classifier [classifier matches anti-matches]
  (-> classifier
      ((partial reduce #(process-document-tokens %1 %2 true)) matches)
      ((partial reduce #(process-document-tokens %1 %2 false)) anti-matches)))

(defn train-classifier! [category & [count]]
  (let [matches (classified-domain-tokens category true count)
        anti-matches (classified-domain-tokens category false count)]
    (update-classifier! category #(train-classifier % matches anti-matches))))

(defn score-tokens [tokens token-probabilities]
  (reduce #(* %1 (inc (or (token-probabilities %2) 0)))
          1
          tokens))

(defn get-domain-classification [category domain]
  (let [{:keys [probabilities anti-probabilities]} (classifier category)
        tokens (tokens domain)]
    (> (score-tokens tokens probabilities)
       (score-tokens tokens anti-probabilities))))

(defn classify-domain! [category domain]
  (update-domain! domain [:bayes-class category]
                  (get-domain-classification category domain)))
