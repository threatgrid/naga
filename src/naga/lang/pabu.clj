(ns ^{:doc "Implements Pabu, which is a Prolog-like language for Naga.
Parses code and returns Naga rules."
      :author "Paula Gearon"}
  naga.lang.pabu
  (:require [naga.schema.structs :as structs :refer [Axiom Program]]
            [naga.lang.parser :as parser]
            [naga.rules :as r]
            [schema.core :as s])
  (:import [java.io InputStream]
           [naga.schema.structs Rule]))

;; TODO: Multi-arity not yet supported
(def Args
  [(s/one s/Any "entity")
   (s/optional s/Any "value")])

(def AxiomAST
  {:type (s/eq :axiom)
   :axiom [(s/one s/Keyword "Property")
           (s/one Args "args")]})

(def Triple
  [(s/one s/Any "entity")
   (s/one s/Any "property")
   (s/one s/Any "value")])

(s/defn triplet :- Triple
  [[property [s o :as args]]]
  (if (= 1 (count args))
    [s :rdf/type property]
    [s property o]))

(s/defn ast->axiom :- Axiom
  "Converts the axiom structure returned from the parser"
  [{axiom :axiom :as axiom-ast} :- AxiomAST]
  (triplet axiom))

(def VK (s/cond-pre s/Keyword s/Symbol))

(def Predicate [(s/one VK "property")
                (s/one Args "arguments")])

(def RuleAST
  {:type (s/eq :rule)
   :head [(s/one VK "property")
          (s/one Args "arguments")]
   :body [Predicate]})

(s/defn ast->rule
  "Converts the rule structure returned from the parser"
  [{:keys [head body] :as rule-ast} :- RuleAST]
  (r/rule (triplet head) (map triplet body) (-> head first name gensym name)))

(s/defn read-str :- {:rules [Rule]
                     :axioms [Axiom]}
  "Reads a string"
  [s :- s/Str]
  (let [program-ast (parser/parse s)
        axioms (filter (comp (partial = :axiom) :type) program-ast)
        rules (filter (comp (partial = :rule) :type) program-ast)]
    {:rules (map ast->rule rules)
     :axioms (map ast->axiom axioms)}))

(s/defn read-stream :- Program
  "Reads a input stream"
  [in :- InputStream]
  (let [text (slurp in)]
    (read-str text)))
