(ns naga.rules
    "Defines rule structures and constructors to keep them consistent"
    (:require [clojure.set :as set]
              [zuko.util :as u]
              [zuko.schema :as ss :refer [EPVPattern Axiom Pattern
                                          vartest? filter-pattern?
                                          op-pattern? eval-pattern?]]
              [naga.schema.structs :as st
                                   :refer #?(:clj  [RulePatternPair Body Program]
                                             :cljs [RulePatternPair Body Program Rule])]
              #?(:clj  [schema.core :as s]
                 :cljs [schema.core :as s :include-macros true]))
    #?(:clj (:import [naga.schema.structs Rule])))

(defn- gen-rule-name [] (name (gensym "rule-")))

(defn- fresh-var
  "Changes a var to a 'fresh' var. These start with % rather than ?"
  [v]
  (symbol (str \% (subs (name v) 1))))

(defn- fresh-var?
  [x]
  (and (symbol? x) (= \% (first (name x)))))

(defn bindings?
  [b]
  (and (vector? (:cols (meta b))) (sequential? b)))

(def operators '#{not NOT or OR and AND})

(defn epv-pattern?
  [pattern]
  (or (ss/epv-pattern? pattern)
      (and (= 3 (count pattern))
           (not (operators (first pattern)))
           (not (some sequential? pattern)))))

(defn get-vars
  [[f & r :as pattern]]
  (cond
    (epv-pattern? pattern) (set (ss/vars pattern))
    (filter-pattern? pattern) (set (filter vartest? f))
    (op-pattern? pattern) (if (operators f)
                            (set (mapcat get-vars r))
                            (throw (ex-info "Unknown operator" {:op f :args r})))
    (eval-pattern? pattern) (set (filter vartest? r))
    :default (throw (ex-info (str "Unknown pattern type in rule: " pattern) {:pattern pattern}))))

(def specials #{\' \*})

(defn strip-special
  [s]
  (let [n (name s)
        l (dec (count n))]
    (if (specials (nth n l))
      (symbol (namespace s) (subs n 0 l))
      s)))

(defn mark-unbound
  "Convert a head to use fresh vars for any vars that are unbound.
   Scans the vars in the body to identify which vars are unbound."
  [head body]
  (let [all-vars (fn [xs]
                   (into #{} (->> xs (reduce #(into %1 (get-vars %2)) #{}) set (map strip-special))))
        head-vars (all-vars head)
        body-vars (all-vars body)
        unbound? (set/difference head-vars body-vars)]
    (map (fn [p] (map #(if (unbound? %) (fresh-var %) %) p)) head)))

(defn var-for*
  [fv]
  (if (fresh-var? fv)
    (->> (gensym "?gen__") str symbol)
    fv))

(defn regen-rewrite
  "Rewrites rules that are generating new entities to avoid them in future iterations.
   This requires the generated entities to be subtracted from the patterns in the rule
   body."
  [head body]
  (let [var-for (memoize var-for*)]
    (letfn [(collect-patterns [p]
              ;; find all head patterns that include fresh vars, and which are connected
              ;; to patterns that are included via fresh vars
              (loop [incvars #{}
                     patterns (set (filter (comp fresh-var? first) head))]
                (let [new-vars (into incvars (mapcat get-vars patterns))
                      new-patterns (set (filter #(and (not (patterns %))
                                                      (some new-vars (get-vars %)))
                                                head))]
                  (if (seq new-patterns)
                    (recur new-vars (into patterns new-patterns))
                    patterns))))
            (var-rewrite [pattern]
              (mapv var-for pattern))]
      (let [patterns-filter (collect-patterns head)
            subtractions (->> (filter patterns-filter head) ;; uses the set to select from the original
                              (map var-rewrite))]
        (if (seq patterns-filter)
          (concat body [(apply list 'not subtractions)])
          body)))))

(s/defn rule :- Rule
  "Creates a new rule"
  ([head body] (rule head body (gen-rule-name)))
  ([head body name]
   (try
     (s/validate [Pattern] body)
     (catch #?(:clj Throwable :cljs :default) e
         (assert (not body) (ex-message e))))
   (assert (and (sequential? head) (or (empty? head) (every? sequential? head)))
           "Head must be a sequence of constraints")
   (assert (every? (complement fresh-var?) (mapcat get-vars body))
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
                      (operators e) e
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

(s/defn collect-patterns :- [EPVPattern]
  "Recurses through a rule body to find all EPV Patterns"
  [body :- Body]
  (let [constraints (remove (comp seq? first) body)]
    (concat (filter vector? constraints)       ;; top level patterns
            (->> constraints
                 (filter seq?)                 ;; nested operations
                 (map rest)                    ;; arguments only
                 (mapcat collect-patterns))))) ;; recurse

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
    (->> sb
         collect-patterns
         (keep matches?))))

(defn dbg [x] (println x) x)

(s/defn create-program :- Program
  "Converts a sequence of rules into a program.
   A program consists of a map of rule names to rules, where the rules have dependencies."
  [rules :- [Rule]
   axioms :- [Axiom]]
  (let [name-bodies (u/mapmap :name :body rules)
        triggers (fn [head] (mapcat (partial find-matches head) name-bodies))
        deps (fn [{:keys [head body name]}]
               (let [body' (regen-rewrite head body)]
                 (st/new-rule head body' name (mapcat triggers head))))]
    {:rules (u/mapmap :name identity (map deps rules))
     :axioms axioms}))

