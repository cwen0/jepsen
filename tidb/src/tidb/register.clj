(ns tidb.register
  "Single atomic register test"
  (:refer-clojure :exclude [test])
  (:require [jepsen [client :as client]
             [checker :as checker]
             [generator :as gen]
             [independent :as independent]
             [util :as util :refer [meh]]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.txn.micro-op :as mop]
            [clojure.java.jdbc :as j]
            [clojure.tools.logging :refer :all]
            [tidb.sql :as c :refer :all]
            [tidb.basic :as basic]
            [knossos.model :as model])
  (:import (knossos.model Model)))

(defn r   [_ _] {:type :invoke, :f :read, :value nil})
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 5)})
(defn cas [_ _] {:type :invoke, :f :cas, :value [(rand-int 5) (rand-int 5)]})

(defrecord RegisterClient [conn]
  client/Client

  (open! [this test node]
    (assoc this :conn (c/open node)))

  (setup! [this test]
    (j/execute! conn ["drop table if exists test"])
    (j/execute! conn ["create table if not exists test
                      (id int primary key, val int)"]))

  (invoke! [this test op]
    (with-txn op [c conn]
      (try
        (let [id   (key (:value op))
              val' (val (:value op))
              val  (-> c (j/query [(str "select * from test where id = ? FOR UPDATE") id] :row-fn :val) first)]
          (case (:f op)
            :read (assoc op :type :ok, :value (independent/tuple id val))

            :write (do
                     (if (nil? val)
                       (j/insert! c :test {:id id :val val'})
                       (j/update! c :test {:val val'} ["id = ?" id]))
                     (assoc op :type :ok))

            :cas (let [[expected-val new-val] val'
                       cnt (j/update! c :test {:val new-val} ["id = ? and val = ?" id expected-val])]
                   (assoc op :type (if (zero? (first cnt))
                                     :fail
                                     :ok))))))))

  (teardown! [this test])

  (close! [this test]
    (c/close! conn)))

(defn register-test-base
  [opts]
  (basic/basic-test
   (merge
    {:client {:client (:client opts)
              :during (:during opts)}
     :checker (checker/compose
               {:perf (checker/perf)
                :indep (independent/checker
                        (checker/compose
                         {:timeline (timeline/html)
                          :linear (checker/linearizable
                                   {:model (:model opts)})}))})}
    (dissoc opts :client :during :model))))

(defn test
  [opts]
  (register-test-base
   (merge {:name "register"
           :client (RegisterClient. nil)
           :during (independent/concurrent-generator
                    10
                    (range)
                    (fn [k]
                      (->> (gen/reserve 5 (gen/mix [w cas cas]) r)
                           (gen/delay-til 1/2)
                           (gen/stagger 1/10)
                           (gen/limit 100))))
           :model (model/cas-register 0)}
          opts)))

(defrecord MultiRegisterClient [conn]
  client/Client

  (open! [this test node]
    (assoc this :conn (c/open node)))

  (setup! [this test]
    (j/execute! conn ["create table if not exists test
                      (id int, val int, ik int, primary key(id, ik))"]))

  (invoke! [this test op]
    (let [[ik txn] (:value op)]
      (case (:f op)
        :read
        (let [ks   (map mop/key txn)
              vs   (->> (j/query conn [(str "select id val from test where ik = " ik " and id in " ks)])
                        (map (juxt :id :val))
                        (into {}))
              txn' (mapv (fn [[f k _]] [f k (get vs k)]) txn)]
          (assoc op :type :ok, :value (independent/tuple ik txn')))
        :write
        (with-txn op [c conn]
          (->> (for [[f k v] txn]
                 (do
                   (assert (= :w f))
                   (j/insert! c :test {:id k :ik ik :val v})))))
        (assoc op :type :ok))))

  (teardown! [this test]))

; Three keys, five possible values per key.
(def key-range (vec (range 10)))
(defn rand-val [] (rand-int 30))

(defn r
  "Read a random subset of keys."
  [_ _]
  (->> (util/random-nonempty-subset key-range)
       (mapv (fn [k] [:r k nil]))
       (array-map :type :invoke, :f :read, :value)))

(defn w [_ _]
  "Write a random subset of keys."
  (->> (util/random-nonempty-subset key-range)
       (mapv (fn [k] [:w k (rand-val)]))
       (array-map :type :invoke, :f :write, :value)))

(defrecord MultiRegister []
  Model
  (step [this op]
    (reduce (fn [state [f k v]]
              ; Apply this particular op
              (case f
                :r (if (or (nil? v)
                           (= v (get state k)))
                     state
                     (reduced
                      (model/inconsistent
                       (str (pr-str (get state k)) "≠" (pr-str v)))))
                :w (assoc state k v)))
            this
            (:value op))))

(defn multi-register
  "A register supporting read and write transactions over registers identified
  by keys. Takes a map of initial keys to values. Supports a single :f for ops,
  :txn, whose value is a transaction: a sequence of [f k v] tuples, where :f is
  :read or :write, k is a key, and v is a value. Nil reads are always legal."
  [values]
  (map->MultiRegister values))

(defn multi-register-test
  [opts]
  (register-test-base
   (merge {:name "multi-register"
           :client (MultiRegisterClient. nil)
           :during (independent/concurrent-generator
                    10
                    (range)
                    (fn [k]
                      (->> (gen/reserve 5 r w)
                           (gen/stagger 1)
                           (gen/limit 20))))
           :model (multi-register {})}
          opts)))

