(ns ^{:doc "Parser for Pabu expressions. This is basic infix arithmetic."
      :author "Paula Gearon"}
  naga.lang.expression
  (:refer-clojure :exclude [char])
  (:require [the.parsatron :refer :all]
            [naga.lang.basic :refer
             [whitespace-char opt-whitespace separator open-paren close-paren
              equals not-equals lt gt lte gte
              plus minus tms divide
              elt
              choice* either*]]))


(def ecomparator (choice* equals not-equals lt gt lte gte))

(def infix-operator (choice* plus minus tms divide))

(def fn-symbol
  {\= =, "!=" not=, \< <, \> >, "<=" <=, ">=" >=,
   \+ +, \- -, \* *, \/ /})

(declare expression)

(defparser op-expression []
  (let->> [op (>> opt-whitespace infix-operator)
           e (>> opt-whitespace (expression))]
    (always [op e])))

(defparser expression []
  (let->> [f (elt)
           r (either (op-expression) (always nil))]
    (always (if r [f r] f))))
