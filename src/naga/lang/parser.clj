(ns ^{:doc "Parser for Pabu, which is a Prolog-like syntax for Naga."
      :author "Paula Gearon"}
  naga.lang.parser
  (:refer-clojure :exclude [char])
  (:require [the.parsatron :refer :all]
            [naga.lang.basic :refer
             [whitespace-char opt-whitespace separator open-paren close-paren
              arg-list elt
              choice* either*]]
            [naga.lang.expression :refer [ecomparator fn-symbol expression]]))

;; a structure is a predicate with arguments, like foo(bar)
(defparser structure []
  (let->> [p (elt)
           args (between open-paren close-paren (arg-list))]
    (always [p args])))

(defparser structures []
  (let->> [s (structure)
           ss (many (attempt (>> separator (structure))))]
    (always (cons s ss))))

(defparser comparison []
  (let->> [lhs (expression)
           c-type (>> opt-whitespace ecomparator)
           rhs (>> opt-whitespace (expression))]
    (always (list (fn-symbol ecomparator) lhs rhs))))

(defparser nonbase-clause []
  (let->> [head (>> opt-whitespace (structure))
           _ (>> opt-whitespace (string ":-") opt-whitespace)
           body (structures)
           _ (>> opt-whitespace (char \.) opt-whitespace)]
    (always {:type :rule
             :head head
             :body body})))

(defparser base-clause []
  (let->> [structure (>> opt-whitespace (structure))
           _ (>> opt-whitespace (char \.) opt-whitespace)]
    (always {:type :axiom
             :axiom structure})))

(def program (many (either* (nonbase-clause) (base-clause))))

(defn parse
  "Parse a string"
  [s]
  (run program s))
