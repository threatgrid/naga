(ns naga.lang.parser
  "Parser for Pabu, which is a Prolog-like syntax for Naga."
  (:refer-clojure :exclude [char])
  (:require [clojure.string :as str]
            #?(:clj [the.parsatron :refer [defparser let->> >> always between many attempt char string run]]
               :cljs [the.parsatron :refer [always between many attempt char string run]
                                    :refer-macros [defparser let->> >>]])
            [naga.lang.basic :refer
             [whitespace-char opt-whitespace separator open-paren close-paren
              get-vars arg-list elt
              choice* either*]]
            [naga.lang.expression :refer [fn-symbol relation expression]]))

(defn vars-for
  "Returns the vars of an expression"
  [x]
  (if (sequential? x)
    (get-vars x)
    (if (symbol? x) [x])))

(defparser relational-expr []
  (let->> [lhs expression
           c-type (>> opt-whitespace relation)
           rhs (>> opt-whitespace expression)]
    (let [vars (-> #{}
                   (into (vars-for lhs))
                   (into (vars-for rhs)))
          expr (with-meta
                 (list (fn-symbol c-type) lhs rhs)
                 {:vars vars})]
      (always expr))))

;; a structure is a predicate with arguments, like foo(bar)
(defparser structure []
  (let->> [p (elt)
           args (between open-paren close-paren (arg-list))]
    (always [p args])))

;; a list of predicates or expressions
(defparser structures []
  (let->> [s (structure)
           ss (many
               (attempt
                (>> separator
                    (either*
                     (relational-expr)
                     (structure)))))]
    (always (cons s ss))))

;; a clause with a rule
(defparser nonbase-clause []
  (let->> [head (>> opt-whitespace (structures))
           _ (>> opt-whitespace (string ":-") opt-whitespace)
           body (structures)
           _ (>> opt-whitespace (char \.) opt-whitespace)]
    (always {:type :rule
             :head head
             :body body})))

;; an axiom
(defparser base-clause []
  (let->> [structure (>> opt-whitespace (structure))
           _ (>> opt-whitespace (char \.) opt-whitespace)]
    (always {:type :axiom
             :axiom structure})))

(def program (many (either* (nonbase-clause) (base-clause))))

(def dblquotes (apply str (map clojure.core/char [733 8220 8221 8223 8243 10077 10078])))

(def dblquote-pattern (re-pattern (str "[" dblquotes "]")))

(defn clean-quotes
  "Convert smart quotes into standard ascii character 34"
  [s]
  (str/replace s dblquote-pattern "\""))

(defn parse
  "Parse a string"
  [s]
  (run program (clean-quotes s)))
