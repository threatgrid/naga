(ns ^{:doc "Implements Pabu, which is a Prolog-like language for Naga.
Parses code and returns Naga rules."
      :author "Paula Gearon"}
  naga.lang.pabu
  (:require [naga.schema.structs :as structs :refer [Axiom Program]]
            [naga.lang.parser :as parser]
            [naga.rules :as r]
            [schema.core :as s])
  (:import [java.io InputStream]
           [naga.schema.structs Rule]
           [clojure.lang Var]))

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

(s/defn get-fn-reference :- (s/maybe Var)
  "Looks up a namespace:name function represented in a keyword,
   and if it exists, return it. Otherwise nil"
  [kw :- s/Keyword]
  (let [kns (namespace kw)
        snm (symbol (name kw))]
    (some-> kns
      symbol
      find-ns
      (ns-resolve snm))))

(s/defn triplets :- [Triple]
  "Converts raw parsed predicate information into a seq of triples"
  [[property [s o :as args]]]
  (case (count args)
    0 [[s :rdf/type :owl/thing]]
    1 [[s :rdf/type property]]
    2 [[s property o]]
    (throw (ex-info "Multi-arity predicates not yet supported"))))

(s/defn triplet :- Triple
  "Converts raw parsed predicate information into a single triple"
  [raw]
  (first (triplets raw)))

(defn structure
  "Converts the AST for a structure into either a seq of triplets or predicates.
   Types are intentionally loose, since it's either a pair or a list."
  [ast-data]
  (if (vector? ast-data)
    (let [[p args] ast-data]
      (if-let [f (and (keyword? p) (get-fn-reference p))]
        [(with-meta (cons f args) (meta args))]
        (triplets ast-data)))
    [ast-data]))

(s/defn ast->axiom :- Axiom
  "Converts the axiom structure returned from the parser"
  [{axiom :axiom :as axiom-ast} :- AxiomAST]
  (triplet axiom))

(def VK "Either a Variable or a Keyword" (s/cond-pre s/Keyword s/Symbol))

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
  (r/rule (triplet head) (mapcat structure body) (-> head first name gensym name)))

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
