(ns p-crawler.classifier
  (:require [monger.core :as mg]
            [monger.collection :as mc]
            [monger.joda-time :refer :all]
            [clojure.string :refer [lower-case]]
            [clojure.set :refer [difference intersection union]]
            [clojure.core.async :refer [go]]
            [p-crawler.database :refer :all])
  (:import [java.net URL]
           [de.l3s.boilerpipe.extractors ArticleExtractor DefaultExtractor]
           [de.l3s.boilerpipe.sax BoilerpipeHTMLParser]
           [de.l3s.boilerpipe.util UnicodeTokenizer]))

(defn extract-content [webpage]
  (.getText DefaultExtractor/INSTANCE webpage))

(defn tokenize [content]
  (vec (map lower-case (UnicodeTokenizer/tokenize content))))

(def stop-words #{"a" "about" "above" "across" "after" "afterwards" "again" "against" "all" "almost" "alone" "along" "already" "also" "although" "always" "am" "among" "amongst" "amoungst" "amount" "an" "and" "another" "any" "anyhow" "anyone" "anything" "anyway" "anywhere" "are" "around" "as" "at" "back" "be" "became" "because" "become" "becomes" "becoming" "been" "before" "beforehand" "behind" "being" "below" "beside" "besides" "between" "beyond" "bill" "both" "bottom" "but" "by" "call" "can" "cannot" "cant" "co" "computer" "con" "could" "couldnt" "cry" "de" "describe" "detail" "do" "done" "down" "due" "during" "each" "eg" "eight" "either" "eleven" "else" "elsewhere" "empty" "enough" "etc" "even" "ever" "every" "everyone" "everything" "everywhere" "except" "few" "fifteen" "fify" "fill" "find" "fire" "first" "five" "for" "former" "formerly" "forty" "found" "four" "from" "front" "full" "further" "get" "give" "go" "had" "has" "hasnt" "have" "he" "hence" "her" "here" "hereafter" "hereby" "herein" "hereupon" "hers" "herse”" "him" "himself" "his" "how" "however" "hundred" "i" "ie" "if" "in" "inc" "indeed" "interest" "into" "is" "it" "its" "itself" "keep" "last" "latter" "latterly" "least" "less" "ltd" "made" "many" "may" "me" "meanwhile" "might" "mill" "mine" "more" "moreover" "most" "mostly" "move" "much" "must" "my" "myse”" "name" "namely" "neither" "never" "nevertheless" "next" "nine" "no" "nobody" "none" "noone" "nor" "not" "nothing" "now" "nowhere" "of" "off" "often" "on" "once" "one" "only" "onto" "or" "other" "others" "otherwise" "our" "ours" "ourselves" "out" "over" "own" "part" "per" "perhaps" "please" "put" "rather" "re" "same" "see" "seem" "seemed" "seeming" "seems" "serious" "several" "she" "should" "show" "side" "since" "sincere" "six" "sixty" "so" "some" "somehow" "someone" "something" "sometime" "sometimes" "somewhere" "still" "such" "system" "take" "ten" "than" "that" "the" "their" "them" "themselves" "then" "thence" "there" "thereafter" "thereby" "therefore" "therein" "thereupon" "these" "they" "thick" "thin" "third" "this" "those" "though" "three" "through" "throughout" "thru" "thus" "to" "together" "too" "top" "toward" "towards" "twelve" "twenty" "two" "un" "under" "until" "up" "upon" "us" "very" "via" "was" "we" "well" "were" "what" "whatever" "when" "whence" "whenever" "where" "whereafter" "whereas" "whereby" "wherein" "whereupon" "wherever" "whether" "which" "while" "whither" "who" "whoever" "whole" "whom" "whose" "why" "will" "with" "within" "without" "would" "yet" "you" "your" "yours" "yourself" "yourselves"}) 

(defn remove-stop-words [tokens]
  (remove stop-words tokens))

(defn webpage-to-token-bag [webpage]
  (-> webpage
      extract-content
      tokenize
      remove-stop-words
      set))

(def classifiers (atom {}))

(defn save-classifier! [{:keys [category] :as classifier}]
  (update-document! "classifiers" [category] classifier))

(defn generate-classifier [category]
  {:category category
   :count 0
   :probabilities {}
   :anti-probabilities {}} )

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

(defn process-document-tokens [{:keys [probabilities anti-probabilities count]} tokens match?]
  (let [old-tokens (set (keys probabilities))
        new-tokens (difference tokens old-tokens)
        incrementing-tokens (intersection tokens old-tokens)
        decrementing-tokens (difference old-tokens tokens)
        inc-probs (transient (if match? probabilities anti-probabilities))
        dec-probs (transient (if match? anti-probabilities probabilities))]
    (doseq [token new-tokens]
      (assoc! inc-probs token (/ 1 (inc count)))
      (assoc! dec-probs token 0))
    (doseq [token incrementing-tokens]
      (assoc! inc-probs
              token
              (increment-token-probability (get inc-probs token) count)))
    (doseq [token decrementing-tokens]
      (assoc! dec-probs
              token
              (decrement-token-probability (get dec-probs token) count)))
    {:count (int count)
     :probabilities (persistent! (if match? inc-probs dec-probs))
     :anti-probabilities (persistent! (if match? dec-probs inc-probs))}))
    
(defn train-classifier [classifier matches antimatches]
  (let [reduce-update (partial reduce process-document-tokens)]
    (-> classifier
        (reduce-update matches (repeat true))
        (reduce-update matches (repeat false)))))
