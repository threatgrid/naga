(ns ^{:doc "Parser for Pabu expressions. This is basic infix arithmetic."
      :author "Paula Gearon"}
  naga.lang.expression
  (:refer-clojure :exclude [char])
  (:require [the.parsatron :refer :all]
            [naga.lang.basic :refer
             [whitespace-char opt-whitespace separator open-paren close-paren
              equals not-equals lt gt lte gte
              plus minus tms divide
              elt pstring
              choice* either*]]))


(def relation (choice* equals not-equals lt gt lte gte))

(def plus-op (either* plus minus))
(def mult-op (either* divide tms))

(def fn-symbol
  {\= =, "!=" not=, \< <, \> >, "<=" <=, ">=" >=,
   \+ +, \- -, \* *, \/ /})

(defn flatten-expr
  [op1 op2 f r]
  (let [p (keep (fn [[o opnd]] (if (= op1 o) opnd)) r)
        m (keep (fn [[o opnd]] (if (= op2 o) opnd)) r)]
    (if (seq p)
      (if (seq m)
        (apply list (fn-symbol op2) (apply list (fn-symbol op1) f p) m)
        (apply list (fn-symbol op1) f p))
      (if (seq m)
        (apply list (fn-symbol op2) f m)
        f))))

(defparser mult-expr []
  (let->> [op (>> opt-whitespace mult-op)
           e (>> opt-whitespace (elt))]
    (always [op e])))

(defparser multiplicative-expr []
  (let->> [unry (elt)
           munry (many (attempt (mult-expr)))]
    (always (flatten-expr \* \/ unry munry))))

(defparser add-expr []
  (let->> [op (>> opt-whitespace plus-op)
           e (>> opt-whitespace (multiplicative-expr))]
    (always [op e])))

(defparser additive-expr []
  (let->> [mult (multiplicative-expr)
           mmult (many (attempt (add-expr)))]
    (always (flatten-expr \+ \- mult mmult))))

(def expression (choice* (additive-expr) pstring))
