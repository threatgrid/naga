(ns ^{:doc
      "Defines a Queue structure that can be added to the tail, and removed from the head.
       Anything already in the queue (compared by ID) will not be added again, but a function can
       be provided that will update the element when it is already present.
       Includes a 'salience' which allows elements to be promoted through the queue ahead
       of less salient elements."
      :author "Paula Gearon"}
    naga.queue
  (:refer-clojure :exclude [pop])
  (:require [schema.core :as s :refer [=>]]))

(defprotocol PQueue
  (q-count [queue] "Returns the number of items in the queue")
  (head [queue] "Returns the head of the queue")
  (pop [queue] "Removes the head from the queue")
  (add
    [queue element]
    [queue update-fn element]
    "Adds an element to the queue if it isn't already present, returning the new queue.
    Uses update-fn on the element if it is already in the queue"))

(def PQueueType (s/pred #(satisfies? PQueue %)))

(s/defn insert-by :- [s/Any]
  "Insert an element into a sequence, by salience.
   If a salience function is provided, then insert the element immediately after all elements
   of lower salience.
   If no salience is provided, then append the element to the end of the seq"
  [s :- [s/Any]
   salience :- (s/maybe (=> s/Num s/Any))   ;; element -> number
   elt :- s/Any]
  (if (and salience (salience elt))
    (let [preamble (take-while #(>= (salience elt) (salience %)) s)]
      (->> (drop (count preamble) s)
           (cons elt)
           (concat preamble)
           (into [])))
    (conj s elt)))

(declare ->SalienceQueue)

(s/defrecord SalienceQueue
  [q :- [s/Any]       ;; The queue data. A vector
   has? :- #{s/Any}   ;; A set that holds the IDs of all elements in the queue. Used for "contains?" operations.
   id-fn :- (=> s/Any s/Any)  ;; element -> ID (id: string, number, etc)  ;; Gets the ID of elements in the queue.
   salience-fn :- (s/maybe (=> s/Num s/Any))  ;; element -> number        ;; Returns the priority for insertion.
  ]
  PQueue
  (q-count [_] (count q))
  (head [_] (first q))
  (pop [_] (->SalienceQueue (into [] (rest q)) (disj has? (id-fn (first q))) id-fn salience-fn))
  (add [e element] (add e identity element))
  (add
    [this update-fn element]
    (let [id (id-fn element)]
      (if (has? id) ;; TODO: can salience be updated on the fly?
        (if (= identity update-fn)
          this  ;; shortcut to avoid redundant work when using identity
          (let [updater-fn (fn [e] (if (= id (id-fn e)) (update-fn e) e))]
            (->SalienceQueue (mapv updater-fn q) has? id-fn salience-fn)))
        (->SalienceQueue (insert-by q salience-fn element)
                         (conj has? id)
                         id-fn
                         salience-fn)))))

(s/defn new-queue
  "Create an empty queue. When called without arguments, salience is ignored,
   and update and ID are just identity."
  ([]
   (new-queue nil identity))
  ([salience-fn :- (s/maybe (=> s/Num s/Any))
    id-fn :- (=> s/Any s/Any)]
   (->SalienceQueue [] #{} id-fn salience-fn)))

(defn drain
  "Pulls everything off a queue into a seq."
  [queue]
  (loop [s [] q queue]
    (if-let [e (head q)]
      (recur (conj s e) (pop q))
      s)))
