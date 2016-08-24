(ns naga.structs
  "Defines the schemas for rule structures"
  (:require [schema.core :as s]
            [naga.util :as u])
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
  (s/if #(= 1 (count %))
    EntityPattern
    EntityPropertyPattern))

(def RulePatternPair [(s/one s/Str "rule-name")
                      (s/one EPVPattern "pattern")])

(def Body [EPVPattern])

(def ConstraintData
  {:last-count s/Num  ;; The count from the previous execution
   :dirty s/Bool})    ;; If the constraint resolution is dirty

(def StatusMap {EPVPattern (s/atom ConstraintData)})

(def StatusMapEntry
  "Convenience for representing a single key/value pair in a StatusMap"
  [(s/one EPVPattern "Pattern from rule body")
   (s/one (s/atom ConstraintData) "count_and_dirty")])

(def Results [[(s/pred (complement symbol?) "Value")]])

;; Rules defined by a horn clause. The head is a simple pattern,
;; the body is conjunction of pattern matches.
;; All rules have a name, and a list of names of downstream rules.
(s/defrecord Rule
    [head :- EPVPattern
     body :- Body
     name :- s/Str
     salience :- s/Num
     downstream :- [RulePatternPair]
     status :- {EPVPattern (s/atom ConstraintData)}
     execution-count :- (s/atom s/Num)])

(defn new-rule
  ([head body name]
   (new-rule head body name []))
  ([head body name downstream]
   (new-rule head body name downstream 0))
  ([head body name downstream salience]
   (->Rule head body name salience downstream
           (u/mapmap (fn [_]
                       (atom {:last-count 0
                              :dirty true}))
                     body)
           (atom 0))))

(def EntityPropAxiomElt
  (s/cond-pre s/Keyword Long))

(def EntityPropValAxiomElt
  (s/conditional (complement symbol?) s/Any))

(def Axiom
  [(s/one EntityPropAxiomElt "entity")
   (s/one EntityPropAxiomElt "property")
   (s/one EntityPropValAxiomElt "value")])

(def Statement (s/cond-pre Axiom Rule))

(def Program
  {(s/required-key :rules) {s/Str Rule}
   (s/required-key :axioms) [Axiom]})
