(ns naga.test-queue
  (:require [naga.queue :as q]
            #?(:clj  [clojure.test :as t :refer [is]]
               :cljs [clojure.test :as t :refer-macros [is]])
            #?(:clj  [schema.test :as st :refer [deftest]]
               :cljs [schema.test :as st :refer-macros [deftest]])))

(t/use-fixtures :once st/validate-schemas)

(deftest simple
  "Test adding to an identity queue, without salience"
  (let [data (shuffle (range 10))
        queue (reduce q/add (q/new-queue) data)]
    (is (= data (q/drain queue)))
    (let [data2 (concat data (shuffle (range 10)))
          queue (reduce q/add (q/new-queue) data2)]
      (is (= data (q/drain queue))))))

(deftest salience
  "Test adding to an identity queue, with salience"
  (let [data (shuffle (range 10))
        queue (reduce q/add (q/new-queue identity identity) data)]
    (is (= (range 10) (q/drain queue)))
    (let [data2 (concat data (shuffle (range 10)))
          queue (reduce q/add (q/new-queue identity identity) data2)]
      (is (= (range 10) (q/drain queue))))))

(def test-data
  [{:id 1 :s 2}
   {:id 2 :s 2}
   {:id 3 :s 2}
   {:id 4 :s 1}
   {:id 5 :s 3}
   {:id 6 :s 2}])

(deftest structure-elts
  (let [queue (reduce q/add (q/new-queue :s :id) test-data)]
    (is (= [4 1 2 3 6 5] (map :id (q/drain queue))))
    (let [q2 (-> queue (q/add {:id 3 :s 2}) (q/add {:id 2 :s 3}))]
      (is (= [4 1 2 3 6 5] (map :id (q/drain q2)))))))

(def update-data
  [{:id 1 :s 2 :data (atom 1)}
   {:id 2 :s 2 :data (atom 1)}
   {:id 3 :s 2 :data (atom 1)}
   {:id 4 :s 1 :data (atom 1)}
   {:id 5 :s 3 :data (atom 1)}
   {:id 6 :s 2 :data (atom 1)}])

(defn updater
  "Updates the :data atom in the element with an increment and returns the element"
  [e]
  (update-in e [:data] swap! inc)
  e)

(defn- adder [queue e] (q/add queue updater e))

(deftest updates
  (let [queue (reduce adder (q/new-queue :s :id) update-data)
        df (comp deref :data)]
    (is (= [1 1 1 1 1 1] (map df (q/drain queue))))
    (let [q2 (-> queue (adder {:id 3}) (adder {:id 5}))]
      (is (= [4 1 2 3 6 5] (map :id (q/drain q2))))
      (is (= [1 1 1 2 1 2] (map df (q/drain q2))))
      (let [q3 (-> q2 (adder {:id 3}) (adder {:id 1}))]
        (is (= [4 1 2 3 6 5] (map :id (q/drain q3))))
        (is (= [1 2 1 3 1 2] (map df (q/drain q3))))
        (let [q4 (-> q3 q/pop q/pop q/pop (adder {:id 4}) (adder {:id 1}) (adder {:id 2}))]
          (is (= [3 6 5 4 1 2] (map :id (q/drain q4)))))))))

#?(:cljs (t/run-tests))
