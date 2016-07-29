(ns naga.test-queue
  (:require [clojure.test :refer :all]
            [naga.queue :as q]
            [schema.core :as s]))

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
        queue (reduce q/add (q/new-queue identity identity identity) data)]
    (is (= (range 10) (q/drain queue)))
    (let [data2 (concat data (shuffle (range 10)))
          queue (reduce q/add (q/new-queue identity identity identity) data2)]
      (is (= (range 10) (q/drain queue))))))

(def test-data
  [{:id 1 :s 2}
   {:id 2 :s 2}
   {:id 3 :s 2}
   {:id 4 :s 1}
   {:id 5 :s 3}
   {:id 6 :s 2}])

(deftest structure-elts
  (let [queue (reduce q/add (q/new-queue :s :id identity) test-data)]
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

(defn updater [e]
  (println "Got: " e)
  #_(update-in e [:data] swap! inc)
  (let [e (update-in e [:data] swap! inc)]
    (println "Returning: " e)
    e))

(deftest updates
  (let [queue (reduce q/add (q/new-queue :s :id identity) update-data)
        df (comp deref :data)]
    (is (= [1 1 1 1 1 1] (map df (q/drain queue))))
    (let [q2 (-> queue (q/add {:id 3}) (q/add {:id 5}))]
      (is (= [4 1 2 3 6 5] (map :id (q/drain q2))))
      (is (= [1 1 1 2 1 2] (map df (q/drain q2))))
      (let [q3 (-> q2 (q/add {:id 3}) (q/add {:id 1}))]
      (is (= [4 1 2 3 6 5] (map :id (q/drain q3))))
      (is (= [1 2 1 3 1 2] (map df (q/drain q3))))))))
