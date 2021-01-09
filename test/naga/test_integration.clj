(ns naga.test-integration
  (:require [clojure.test :refer :all]
            [clojure.string :as string]
            [naga.cli :refer :all]
            [cheshire.core :as json])
  (:import [java.io StringWriter]))

(def out *out*)

(defn capture-output
  [f & args]
  (with-open [out-buffer (StringWriter.)
              err-buffer (StringWriter.)]
    (binding [*out* out-buffer
              *err* err-buffer]
      (let [a (if (= 1 (count args))
                (remove empty? (string/split (first args) #"\s"))
                args)]
        (try
          (apply f a)
          (catch Exception e
            (binding [*out* out]
              (println (.toString out-buffer))
              (println (.toString err-buffer)))
            (throw e)))
        [(.toString out-buffer) (.toString err-buffer)]))))

(def family-out
  "INPUT DATA\nsibling(fred, barney).\nparent(fred, mary).\nsibling(mary, george).\ngender(george, male).\n\nNEW DATA\nuncle(fred, george).\nbrother(mary, george).\nparent(barney, mary).\nuncle(barney, george).\nsibling(barney, fred).\n")

(def family-2nd-out
  "INPUT DATA\nsibling(fred, barney).\nparent(fred, mary).\nsibling(mary, george).\ngender(george, male).\nowl:SymmetricProperty(sibling).\n\nNEW DATA\nuncle(fred, george).\nbrother(mary, george).\nsibling(george, mary).\nparent(barney, mary).\nuncle(barney, george).\nsibling(barney, fred).\n")

(deftest test-basic-program
  (let [[out err] (capture-output -main "-n example_data/family.lg")]
    (is (= out family-out))
    (is (= err "")))
  (let [[out err] (capture-output -main "-n example_data/family-2nd-ord.lg")]
    (is (= out family-2nd-out))
    (is (= err ""))))

(def json-out
  [{:id "barney", :parent "mary", :uncle "george", :sibling "fred"}
   {:id "fred", :sibling "barney", :parent "mary", :uncle "george"}
   {:id "george", :type "male", :sibling "mary"}
   {:id "mary", :sibling "george", :brother "george"}])

(deftest test-json-flow
  (let [[out err] (capture-output -main "--json test/naga/data/in.json --out test/tmp/out.json test/naga/data/json-family.lg -n")
        json-result (json/parse-string (slurp "test/tmp/out.json") keyword)]
    (is (empty? out))
    (is (empty? err))
    (is (= (sort-by :id json-result) json-out))))
