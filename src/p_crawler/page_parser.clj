(ns p-crawler.page-parser
  (:require [net.cgrand.enlive-html :as html]
            [clojure.string :refer [lower-case]]
            [clojure.set :refer [difference intersection union]])
  (:import [de.l3s.boilerpipe.extractors ArticleExtractor DefaultExtractor KeepEverythingExtractor]
           [de.l3s.boilerpipe.sax BoilerpipeHTMLParser]
           [de.l3s.boilerpipe.util UnicodeTokenizer]))

(defn extract-links [body]
  (map (comp :href :attrs)
       (html/select
        (html/html-resource (java.io.StringReader. body))
        [:a])))

(defn extract-content [webpage]
  (.getText KeepEverythingExtractor/INSTANCE webpage))

(defn remove-unnecessary-punctuation [token]
  (let [lst (last token)]
    (if (or (= lst \.) (= lst \:))
      (subs token 0 (dec (count token)))
      token)))

(defn tokenize [content]
  (vec (map (comp remove-unnecessary-punctuation lower-case)
            (UnicodeTokenizer/tokenize content))))

(def stop-words #{"a" "about" "above" "across" "after" "afterwards" "again" "against" "all" "almost" "alone" "along" "already" "also" "although" "always" "am" "among" "amongst" "amoungst" "amount" "an" "and" "another" "any" "anyhow" "anyone" "anything" "anyway" "anywhere" "are" "around" "as" "at" "back" "be" "became" "because" "become" "becomes" "becoming" "been" "before" "beforehand" "behind" "being" "below" "beside" "besides" "between" "beyond" "bill" "both" "bottom" "but" "by" "call" "can" "cannot" "cant" "co" "computer" "con" "could" "couldnt" "cry" "de" "describe" "detail" "do" "done" "down" "due" "during" "each" "eg" "eight" "either" "eleven" "else" "elsewhere" "empty" "enough" "etc" "even" "ever" "every" "everyone" "everything" "everywhere" "except" "few" "fifteen" "fify" "fill" "find" "fire" "first" "five" "for" "former" "formerly" "forty" "found" "four" "from" "front" "full" "further" "get" "give" "go" "had" "has" "hasnt" "have" "he" "hence" "her" "here" "hereafter" "hereby" "herein" "hereupon" "hers" "herse”" "him" "himself" "his" "how" "however" "hundred" "i" "ie" "if" "in" "inc" "indeed" "interest" "into" "is" "it" "its" "itself" "keep" "last" "latter" "latterly" "least" "less" "ltd" "made" "many" "may" "me" "meanwhile" "might" "mill" "mine" "more" "moreover" "most" "mostly" "move" "much" "must" "my" "myse”" "name" "namely" "neither" "never" "nevertheless" "next" "nine" "no" "nobody" "none" "noone" "nor" "not" "nothing" "now" "nowhere" "of" "off" "often" "on" "once" "one" "only" "onto" "or" "other" "others" "otherwise" "our" "ours" "ourselves" "out" "over" "own" "part" "per" "perhaps" "please" "put" "rather" "re" "same" "see" "seem" "seemed" "seeming" "seems" "serious" "several" "she" "should" "show" "side" "since" "sincere" "six" "sixty" "so" "some" "somehow" "someone" "something" "sometime" "sometimes" "somewhere" "still" "such" "system" "take" "ten" "than" "that" "the" "their" "them" "themselves" "then" "thence" "there" "thereafter" "thereby" "therefore" "therein" "thereupon" "these" "they" "thick" "thin" "third" "this" "those" "though" "three" "through" "throughout" "thru" "thus" "to" "together" "too" "top" "toward" "towards" "twelve" "twenty" "two" "un" "under" "until" "up" "upon" "us" "very" "via" "was" "we" "well" "were" "what" "whatever" "when" "whence" "whenever" "where" "whereafter" "whereas" "whereby" "wherein" "whereupon" "wherever" "whether" "which" "while" "whither" "who" "whoever" "whole" "whom" "whose" "why" "will" "with" "within" "without" "would" "yet" "you" "your" "yours" "yourself" "yourselves"}) 

(defn remove-stop-words [tokens]
  (remove stop-words tokens))

(defn webpage-to-token-bag [webpage]
  (-> webpage
      extract-content
      tokenize
      remove-stop-words
      set))
