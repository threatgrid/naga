(ns naga.schema.structs
  #?(:clj (:require [schema.core :as s]
                    [naga-store.schema.structs :as nss :refer [EPVPattern Pattern Axiom]])
     :cljs (:require [schema.core :as s :include-macros true]
                     [naga-store.schema.structs :as nss]))
  #?(:cljs (:import [naga-store.schema.structs EPVPattern Pattern Axiom])))

(def RulePatternPair [(s/one s/Str "rule-name")
                      (s/one EPVPattern "pattern")])

(def Body [Pattern])
(def Head [EPVPattern])

(def ConstraintData
  {:last-count s/Num  ;; The count from the previous execution
   :dirty s/Bool})    ;; If the constraint resolution is dirty

(def StatusMap {EPVPattern (s/atom ConstraintData)})

(def StatusMapEntry
  "Convenience for representing a single key/value pair in a StatusMap"
  [(s/one EPVPattern "Pattern from rule body")
   (s/one (s/atom ConstraintData) "count_and_dirty")])

;; Rules defined by a horn clause. The head is a simple pattern,
;; the body is conjunction of pattern matches.
;; All rules have a name, and a list of names of downstream rules.
(s/defrecord Rule
    [head :- Head
     body :- Body
     name :- s/Str
     downstream :- [RulePatternPair]
     salience :- s/Num])

(s/defrecord DynamicRule
    [head :- Head
     body :- Body
     name :- s/Str
     downstream :- [RulePatternPair]
     salience :- s/Num
     status :- {EPVPattern (s/atom ConstraintData)}
     execution-count :- (s/atom s/Num)])

(s/defn new-rule
  ([head :- Head
    body :- Body
    name :- s/Str]
   (new-rule head body name []))
  ([head :- Head
    body :- Body
    name :- s/Str
    downstream :- [RulePatternPair]]
   (new-rule head body name downstream 0))
  ([head :- Head
    body :- Body
    name :- s/Str
    downstream :- [RulePatternPair]
    salience :- s/Num]
   (->Rule head body name downstream salience))
  ([head :- Head
    body :- Body
    name :- s/Str
    downstream :- [RulePatternPair]
    salience :- s/Num
    status :- {EPVPattern (s/atom ConstraintData)}
    execution-count :- (s/atom s/Num)]
   (->DynamicRule head body name downstream salience status execution-count)))

(def Statement (s/cond-pre Axiom Rule))

(def Program
  {(s/required-key :rules) {s/Str Rule}
   (s/required-key :axioms) [Axiom]})

(def RunnableProgram
  {(s/required-key :rules) {s/Str DynamicRule}
   (s/required-key :axioms) [Axiom]})
