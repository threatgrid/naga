(ns naga.rules
    "Defines rule structures and constructors to keep them consistent"
    (:require [clojure.set :as set]
              [naga.util :as u]
              [naga.schema.store-structs :as ss :refer [EPVPattern Axiom]]
              [naga.schema.structs :as st
                                   :refer #?(:clj [RulePatternPair Body Program]
                                             :cljs [RulePatternPair Body Program Rule])]
              #?(:clj  [schema.core :as s]
                 :cljs [schema.core :as s :include-macros true])
              )
    #?(:clj (:import [naga.schema.structs Rule])))

(defn- gen-rule-name [] (name (gensym "rule-")))

(defn- fresh-var
  "Changes a var to a 'fresh' var. These start with % rather than ?"
  [v]
  (symbol (str \% (subs (name v) 1))))

(defn- fresh-var?
  [x]
  (and (symbol? x) (= \% (first (name x)))))

(defn- vars [constraint]
  (if (list? constraint)
    (filter ss/vartest? (rest constraint))
    (ss/vars constraint)))

(defn mark-unbound
  "Convert a head to use fresh vars for any vars that are unbound.
   Scans the vars in the body to identify which vars are unbound."
  [head body]
  (let [all-vars (fn [xs] (set (mapcat (partial filter ss/vartest?) xs)))
        head-vars (all-vars head)
        body-vars (all-vars body)
        unbound? (set/difference head-vars body-vars)]
    (map (fn [p] (map #(if (unbound? %) (fresh-var %) %) p)) head)))

(s/defn rule :- Rule
  "Creates a new rule"
  ([head body] (rule head body (gen-rule-name)))
  ([head body name]
   (assert (and (sequential? body) (or (empty? body) (every? sequential? body)))
           "Body must be a sequence of constraints")
   (assert (and (sequential? head) (or (empty? head) (every? sequential? head)))
           "Head must be a sequence of constraints")
   (assert (every? (complement fresh-var?) (mapcat vars body))
           "Fresh vars are not allowed in a body")
   (st/new-rule (mark-unbound head body) body name)))

(s/defn named-rule :- Rule
  "Creates a rule the same as an existing rule, with a different name."
  [name :- Rule
   {:keys [head body salience downstream]} :- s/Str]
  (st/new-rule (mark-unbound head body) body name downstream salience))

(defn- resolve-element
  "Takes a keyword or a symbol and resolve it as a function.
   Only namespaced keywords get converted.
   Symbols default to the clojure.core namespace when no namespace is present.
   Symbols starting with ? are not converted.
   Anything unresolvable is not converted."
  [e]
  (or (cond
        (keyword? e) (u/get-fn-reference e)
        (symbol? e) (cond
                      (namespace e) (u/get-fn-reference e)
                      (= \? (first (name e))) e
                      :default (u/get-fn-reference
                                (symbol "clojure.core" (name e)))))
      e))

(defn- de-ns
  "Remove namespaces from symbols in a pattern"
  [pattern]
  (if (vector? pattern)
    (letfn [(clean [e] (if (symbol? e) (symbol (name e)) e))]
      (apply vector (map clean pattern)))
    (map resolve-element pattern)))

(defmacro r
  "Create a rule, with an optional name.
   Var symbols need not be quoted."
  [& [f :as rs]]
  (let [[nm# rs#] (if (string? f) [f (rest rs)] [(gen-rule-name) rs])
        not-sep# (partial not= :-)
        head# (map de-ns (take-while not-sep# rs#))
        body# (map de-ns (rest (drop-while not-sep# rs#)))]
    `(rule (quote ~head#) (quote ~body#) ~nm#)))

(defn check-symbol
  "Asserts that symbols are unbound variables for a query. Return true if it passes."
  [sym]
  (let [n (name sym)]
    (assert (#{\? \%} (first n)) (str "Unknown symbol type in rule: " n)) )
  true)

(defn compatible
  [x y]
  (if (symbol? x)
    (and (not (fresh-var? x)) (not (fresh-var? y)) (check-symbol x))
    (or (= x y) (and (symbol? y) (not (fresh-var? y)) (check-symbol y)))))

(s/defn match? :- s/Bool
  "Does pattern a match pattern b?"
  [a :- EPVPattern, b :- EPVPattern]
  (every? identity (map compatible a b)))

(s/defn find-matches :- [RulePatternPair]
  "returns a sequence of name/pattern pairs where a matches a pattern in a named rule"
  [a :- EPVPattern,
   [nm sb] :- [(s/one s/Str "rule-name") (s/one Body "body")]]
  (letfn [(matches? [b]
            "Return a name/pattern if a matches the pattern in b"
            (if (match? a b) [nm b]))]
    (keep matches? sb)))

(defn dbg [x] (println x) x)

(s/defn create-program :- Program
  "Converts a sequence of rules into a program.
   A program consists of a map of rule names to rules, where the rules have dependencies."
  [rules :- [Rule]
   axioms :- [Axiom]]
  (let [name-bodies (u/mapmap :name :body rules)
        triggers (fn [head] (mapcat (partial find-matches head) name-bodies))
        deps (fn [{:keys [head body name]}]
               (st/new-rule head body name (mapcat triggers head)))]
    {:rules (u/mapmap :name identity (map deps rules))
     :axioms axioms}))


