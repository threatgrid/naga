(ns naga.lang.pabu
  "Implements Pabu, which is a Prolog-like language for Naga.  Parses code and returns Naga rules."
  (:require [naga.schema.store-structs :refer [Axiom Triple EPVPattern Pattern FilterPattern]]
            [naga.schema.structs :as structs :refer #?(:clj [Program]
                                                       :cljs [Program Rule])]
            [naga.lang.parser :as parser]
            [naga.rules :as r]
            [naga.util :as u]
            [clojure.string :as str]
            #?(:clj [schema.core :as s]
               :cljs [schema.core :as s :include-macros true]))
  #?(:clj (:import [java.io InputStream]
                   [naga.schema.structs Rule])))

;; TODO: Multi-arity not yet supported
(def Args
  [(s/one s/Any "entity")
   (s/optional s/Any "value")])

(def AxiomAST
  {:type (s/eq :axiom)
   :axiom [(s/one s/Keyword "Property")
           (s/one Args "args")]})

(s/defn triplets :- [Triple]
  "Converts raw parsed predicate information into a seq of triples"
  [[property [s o :as args]]]
  (case (count args)
    0 [[s :rdf/type :owl/thing]]
    1 [[s :rdf/type property]]
    2 [[s property o]]
    (throw (ex-info "Multi-arity predicates not yet supported" {:args args}))))

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
      (if-let [f (and (keyword? p) (u/get-fn-reference p))]
        [(with-meta (cons f args) (meta args))]
        (triplets ast-data)))
    ;; a filter predicate. Wrap in extra vector for syntax purposes
    [[ast-data]]))

(s/defn ast->axiom :- Axiom
  "Converts the axiom structure returned from the parser"
  [{axiom :axiom :as axiom-ast} :- AxiomAST]
  (triplet axiom))

(def VK "Either a Variable or a Keyword" (s/cond-pre s/Keyword s/Symbol))

(def PatternPredicate [(s/one VK "property")
                       (s/one Args "arguments")])

(def ExpressionPredicate (s/pred list?))

(def Predicate (s/cond-pre ExpressionPredicate PatternPredicate))

(def RuleAST
  {:type (s/eq :rule)
   :head [[(s/one VK "property")
            (s/one Args "arguments")]]
   :body [Predicate]})

(s/defn ast->rule :- Rule
  "Converts the rule structure returned from the parser"
  [{:keys [head body] :as rule-ast} :- RuleAST]
  (r/rule (map triplet head)
          (mapcat structure body)
          (-> head ffirst name gensym name)))

(s/defn read-str :- {:rules [Rule]
                     :axioms [Axiom]}
  "Reads a string"
  [s :- s/Str]
  (let [program-ast (parser/parse s)
        axioms (filter (comp (partial = :axiom) :type) program-ast)
        rules (filter (comp (partial = :rule) :type) program-ast)]
    {:rules (map ast->rule rules)
     :axioms (map ast->axiom axioms)}))

#?(:clj
  (s/defn read-stream :- Program
    "Reads a input stream"
    [in :- InputStream]
    (let [text (slurp in)]
    (read-str text))))


;;;; output

(def builtins
  {= "="
   not= "!="
   < "<"
   > ">"
   >= ">="
   <= "<="})

(def builtin-labels (set (vals builtins)))

(defn ps
  [e]
  (letfn [(pabu-var [e] (str (str/upper-case (second e)) (subs e 2)))]
    (cond
      (symbol? e) (let [n (name e)]
                    (if (= \? (first n)) (pabu-var n) n))
      (keyword? e) (subs (str e) 1)
      :default (if-let [l (builtins e)] l e))))

(s/defn predicate->string :- s/Str
  [epv :- EPVPattern]
  (let [[e p v] (map ps epv)]
    (case (count epv)
      1 e
      2 (str p "(" e ")")
      3 (if (= :rdf/type p) (str v "(" e ")") (str p "(" e ", " v ")")))))

(s/defn filter->string :- s/Str
  [[p] :- FilterPattern]
  (let [args (map ps p)]
    (if (builtin-labels (first args))
      (let [[op l r] args]
        (str l " " op " " r))
      (str "(" (str/join " " args) ")"))))

(s/defn pattern->string :- s/Str
  [p :- Pattern]
  (if (vector? p)
    (if (list? (first p))
      (filter->string p)
      (predicate->string p))
    (throw (ex-info "Unknown structure type" {:pattern p}))))

(s/defn rule->str :- s/Str
  "Creates a textual representation for the rule"
  ([rule :- Rule] (rule->str rule false))
  ([{:keys [head body name] :as rule} :- Rule, include-name? :- s/Bool]
   (let [main (str (str/join ", " (map predicate->string head))
                   " :- "
                   (str/join ", " (map pattern->string body))
                   ".")]
     (if include-name? (str main "    /* " name " */") main))))
