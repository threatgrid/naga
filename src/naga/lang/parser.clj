(ns ^{:doc "Parser for Pabu, which is a Prolog-like syntax for Naga."
      :author "Paula Gearon"}
  naga.lang.parser
  (:refer-clojure :exclude [char])
  (:require [the.parsatron :refer :all]))

(defn choice* 
  "choice with backtracking."
  [& args]
  (apply choice (map attempt args)))

(defn either*
  "either with backtracking."
  [p q]
  (either (attempt p) (attempt q)))

(defn upper-case-letter?
  "Prolog considers underscores to be equivalent to an uppercase letter"
  [c]
  (or (Character/isUpperCase ^Character c) (= \_ c)))

(defn upper-case-letter
  []
  (token upper-case-letter?))

(def non-star (token (complement #{\*})))
(def non-slash (token (complement #{\/})))

(defparser cmnt []
  (let->> [_ (>> (string "/*") (many non-star) (many1 (char \*)))
           _ (many (>> non-slash (many non-star) (many1 (char \*))))
           _ (char \/)]
    (always :cmnt)))

(def whitespace-char (token #{\space \newline \tab}))
(def opt-whitespace (many (either whitespace-char (cmnt))))
(def separator (>> opt-whitespace (char \,) opt-whitespace))
(def open-paren (>> (char \() opt-whitespace))
(def close-paren (>> opt-whitespace (char \))))

(def word (many1 (letter)))

(def digits (many1 (digit)))

(defparser signed-digits []
  (let->> [s (token #{\+ \-})
           ds digits]
    (always (cons s ds))))

(defparser integer []
  (let->> [i (either digits (signed-digits))]
    (always (Long/parseLong (apply str i)))))

(defparser floating-point []
  (let->> [i (either digits (signed-digits))
           f (>> (char \.) (many1 (digit)))]
    (always (Double/parseDouble (apply str (apply str i) \. f)))))

(def number (either* (floating-point) (integer)))

(defparser pstring1 []
  (let->> [s (many1 (between (char \') (char \') (many (any-char)))) ]
    (always (flatten (interpose \' s)))))

(defparser pstring2 []
  (let->> [s (many1 (between (char \") (char \") (many (any-char))))]
    (always (flatten (interpose \" s)))))

(def pstring (either (pstring1) (pstring2)))

(defparser variable []
  (let->> [f (upper-case-letter)
           r (many (letter))]
    (always (symbol (apply str "?" (Character/toLowerCase f) r) ))))

(defparser kw []
  (let->> [r word]
    (always (keyword (apply str r)))))

(defparser atm []
  (choice (kw) pstring number))

(defparser elt []
  (choice (variable) (atm)))

(defparser arg-list []
  (let->> [f (elt)
           r (many (>> separator (elt)))]
    (always (cons f r))))

(defparser structure []
  (let->> [p (elt)
           args (between open-paren close-paren (arg-list))]
    (always [p args])))

(defparser structures []
  (let->> [s (structure)
           ss (many (attempt (>> separator (structure))))]
    (always (cons s ss))))

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
