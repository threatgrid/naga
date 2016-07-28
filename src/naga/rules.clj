(ns naga.rules
  "Defines rule structures and constructors to keep them consistent"
  (:require [schema.core :as s]
            [naga.structs :as st :refer [EPVPattern RulePatternPair Body]]
            [naga.util :as u])
  (:import [clojure.lang Symbol]
           [naga.structs Rule]))

(defn- gen-rule-name [] (gensym "rule-"))

(s/defn rule :- Rule
  "Creates a new rule"
  ([head body] (rule head body (gen-rule-name)))
  ([head body name]
   (assert (and (sequential? body) (or (empty? body) (every? sequential? body)))
           "Body must be a sequence of constraints")
   (assert (sequential? head) "Head must be a constraint")
   (st/new-rule head body name)))

(s/defn named-rule :- Rule
  "Creates a rule the same as an existing rule, with a different name."
  [name :- Rule
   {:keys [head body downstream]} :- s/Str]
  (st/new-rule head body name downstream))

(defn- de-ns
  "Remove namespaces from symbols in a pattern"
  [pattern]
  (letfn [(clean [e] (if (symbol? e) (symbol (name e)) e))]
    (apply vector (map clean pattern))))

(defmacro r
  "Create a rule, with an optional name.
   Var symbols need not be quoted."
  [& [f :as rs]]
  (let [[nm# rs#] (if (string? f) [f (rest rs)] [(gen-rule-name) rs])
        not-sep# (partial not= :-)
        head# (de-ns (first (take-while not-sep# rs#)))
        body# (map de-ns (rest (drop-while not-sep# rs#)))]
    `(rule (quote ~head#) (quote ~body#) ~nm#)))

(defn check-symbol
  "Asserts that symbols are unbound variables for a query. Return true if it passes."
  [sym]
  (let [n (name sym)]
    (assert (= \? (first n)) (str "Unknown symbol type in rule: " n)) )
  true)

(defprotocol Matching
  (compatible [x y] "Returns true if both elements are compatible"))

(extend-protocol Matching
  Symbol
  (compatible [x _]
    (check-symbol x))
  Object
  (compatible [x y]
    (or (= x y) (and (symbol? y) (check-symbol y)))))

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

(def ^:private new-state {:symbol-map {} :cntr 0})

(defn- skolemize-symbol
  "Used by skolemize to find or create a new symbol to replace any var symbols.
   element - an element that may contain a symbol. If so, it will be replaced.
   state - keeps track of symbols replacements.
   result: the element, or its replacement, and the new state after replacement."
  [element {:keys [symbol-map cntr] :as state}]
  (if (and (symbol? element) (= \? (first (name element))))
    (if-let [mapped-symbol (symbol-map element)]
      [mapped-symbol state]
      (let [mapped-symbol (symbol (str "?v" cntr))]
        [mapped-symbol {:symbol-map (assoc symbol-map element mapped-symbol)
                        :cntr (inc cntr)}]))
    [element state]))
  
(s/defn skolemize* :- EPVPattern
  "Skolemize a pattern to use consistent naming"
  [pattern :- EPVPattern]
  (->> pattern
       (reduce (fn [[result-pattern state] elt]
                 (let [[skol-sym new-state] (skolemize-symbol elt state)]
                   [(conj result-pattern skol-sym) new-state]))
               [[] new-state])
       first))

(def skolemize (memoize skolemize*))

(defn dbg [x] (println x) x)

(s/defn create-program :- {s/Str Rule}
  "Converts a sequence of rules into a program.
   A program consists of a map of rule names to rules, where the rules have dependencies."
  [rules :- [Rule]]
  (let [name-bodies (u/mapmap :name :body rules)
        triggers (fn [head] (mapcat (partial find-matches head) name-bodies))
        deps (fn [{:keys [head body name]}]
               (st/new-rule head body name (triggers head)))]
    (u/mapmap :name (map deps rules))))


