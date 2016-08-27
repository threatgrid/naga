(ns ^{:doc "Implements Pabu, which is a Prolog-like language for Naga.
Parses code and returns Naga rules."
      :author "Paula Gearon"}
  naga.lang.pabu
  (:require [naga.schema.structs :as structs]
            [naga.lang.parser :as parser]))

(defn read-str
  "Reads a string"
  [s]
  (let [program (parser/parse s)]
    program))

(defn read-stream
  "Reads a input stream"
  [in]
  (let [text (slurp in)]
    (read-str text)))
