(ns naga.queue
  (:refer-clojure :exclude [pop])
  (:require [schema.core :as s :refer [=>]]))

(defprotocol PQueue
  (q-count [queue] "Returns the number of items in the queue")
  (head [queue] "Returns the head of the queue")
  (pop [queue] "Removes the head from the queue")
  (add [queue element] "Adds an element to the queue if it isn't already present, returning the new queue"))

(s/defn insert-by :- [s/Any]
  [s :- [s/Any]
   salience :- (=> s/Num s/Any)          ;; element -> number
   e :- s/Any]
  (let [preamble (if salience
                   (take-while #(>= (salience e) (salience %)) s)
                   s)]
    (concat preamble
            (cons e
                  (drop (count preamble) s)))))

(defrecord SalienceQueue
    [q           ;; :- [s/Any]
     h           ;; :- #{s/Any}
     id-fn       ;; :- (=> s/Any s/Any)  ;; element -> ID (id: string, number, etc)
     salience-fn ;; :- (=> s/Num s/Any)  ;; element -> number
     update-fn   ;; :- (=> s/Any s/Any)  ;; element -> element
     ]
  PQueue
  (q-count [_] (count q))
  (head [_] (first q))
  (pop [_] (->SalienceQueue (rest q) (remove (first q) h) id-fn salience-fn update-fn))
  (add [_ element]
    (let [id (id-fn element)
          updater-fn (fn [e] (if (= id (id-fn e)) (update-fn e) e))]
      (if (h id)  ;; TODO: can salience be updated on the fly?
        (->SalienceQueue (map updater-fn q) h id-fn salience-fn update-fn)
        (->SalienceQueue (insert-by q salience-fn element)
                         (conj h id)
                         id-fn
                         salience-fn
                         update-fn)))))

(s/defn new-queue
  "Create an empty queue. When called without arguments, salience is ignored,
   and update and ID are just identity."
  ([]
   (new-queue nil identity identity))
  ([salience-fn :- (=> s/Num s/Any)
    id-fn :- (=> s/Any s/Any)
    update-fn :- (=> s/Any s/Any)]
   (->SalienceQueue '() #{} id-fn salience-fn update-fn)))

(defn drain
  "Pulls everything off a queue into a seq."
  [queue]
  (loop [s [] q queue]
    (if-let [e (head q)]
      (recur (conj s e) (pop q))
      s)))
