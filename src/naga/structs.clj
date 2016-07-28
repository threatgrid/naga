(ns naga.structs
  "Defines the schemas for rule structures"
  (:require [schema.core :as s])
  (:import [clojure.lang Symbol]))

;; single element in a rule
(def EntityPropertyElt
  (s/cond-pre s/Keyword s/Symbol Long))

;; simple pattern containing a single element. e.g. [?v]
(def EntityPattern [(s/one s/Symbol "entity")])

;; two or three element pattern.
;; e.g. [?s :property]
;;      [:my/id ?property ?value]
(def EntityPropertyPattern
  [(s/one EntityPropertyElt "entity")
   (s/one EntityPropertyElt "property")
   (s/optional s/Any "value")])

;; The full pattern definition, with 1, 2 or 3 elements
(def EPVPattern
  (s/if #(= 1 (count %)) EntityPattern EntityPropertyPattern))

(def RulePatternPair [(s/one s/Str "rule-name")
                      (s/one EPVPattern "pattern")])

(def Body [EPVPattern])

;; Rules defined by a horn clause. The head is a simple pattern,
;; the body is conjunction of pattern matches.
;; All rules have a name, and a list of names of downstream rules.
(s/defrecord Rule
    [head :- EPVPattern
     body :- Body
     name :- s/Str
     downstream :- [RulePatternPair]])

(defn new-rule
  ([head body name]
   (new-rule head body name []))
  ([head body name downstream]
   (->Rule head body name downstream)))
