(ns tidb.sequential
  (:refer-clojure :exclude [test])
  (:require [jepsen [client :as client]
             [checker :as checker]
             [core :as jepsen]
             [generator :as gen]
             [independent :as independent]
             [util :as util :refer [meh]]
             [reconnect :as rc]]
            [clojure.java.jdbc :as j]
            [clojure.set :as set]
            [clojure.tools.logging :refer :all]
            [tidb.sql :refer :all]
            [tidb.basic :as basic]
            [knossos.model :as model]
            [knossos.op :as op]
            [clojure.core.reducers :as r]))

(def table-prefix "String prepended to all table names." "seq_")

(defn table-names
  "Names of all tables"
  [table-count]
  (map (partial str table-prefix) (range table-count)))

(defn key->table
  "Turns a key into a table id"
  [table-count k]
  (str table-prefix (mod (hash k) table-count)))

(defn subkeys
  "The subkeys used for a given key, in order."
  [key-count k]
  (mapv (partial str k "_") (range key-count)))

(defrecord SequentialClient [table-count tbl-created? node]
  client/Client
  (setup! [this test node]
    (Thread/sleep 2000)
    (locking tbl-created?
      (when (compare-and-set! tbl-created? false true)
        ; use first node to exec ddl
        (j/with-db-connection [c (conn-spec (first (:nodes test)))]
          (info "Creating tables" (pr-str (table-names table-count)))
          (doseq [t (table-names table-count)]
            (j/execute! c [(str "drop table if exists " t)])
            (j/execute! c [(str "create table if not exists " t
                                " (tkey varchar(255) primary key)")])
            (info "Created table" t)))))
    (assoc this :node node))

  (invoke! [this test op]
    (with-conn [c node]
      (let [ks (subkeys (:key-count test) (:value op))]
        (case (:f op)
          :write
          (do (doseq [k ks]
                (let [table (key->table table-count k)]
                  (with-txn-retries
                    (j/insert! c table {:tkey k}))))
              (assoc op :type :ok))
          :read
          (->> ks
               reverse
               (mapv (fn [k]
                       (first
                        (with-txn-retries
                          (j/query c [(str "select tkey from "
                                           (key->table table-count k)
                                           " where tkey = ?") k]
                                   :row-fn :tkey)))))
               (vector (:value op))
               (assoc op :type :ok, :value))))))

  (teardown! [this test]))

(defn trailing-nil?
  "Does the given sequence contain a nil anywhere after a non-nil element?"
  [coll]
  (some nil? (drop-while nil? coll)))

(defn checker
  []
  (reify checker/Checker
    (check [this test model history opts]
      (assert (integer? (:key-count test)))
      (let [reads (->> history
                       (r/filter op/ok?)
                       (r/filter #(= :read (:f %)))
                       (r/map :value)
                       (into []))
            none (filter (comp (partial every? nil?) second) reads)
            some (filter (comp (partial some nil?) second) reads)
            bad  (filter (comp trailing-nil? second) reads)
            all  (filter (fn [[k ks]]
                           (= (subkeys (:key-count test) k)
                              (reverse ks)))
                         reads)]
        {:valid?      (not (seq bad))
         :all-count   (count all)
         :some-count  (count some)
         :none-count  (count none)
         :bad-count   (count bad)
         :bad         bad}))))

(defn writes
  "We emit sequential integer keys for writes, logging the most recent n keys
  in the given atom, wrapping a PersistentQueue."
  [last-written]
  (let [k (atom -1)]
    (reify gen/Generator
      (op [this test process]
        (let [k (swap! k inc)]
          (swap! last-written #(-> % pop (conj k)))
          {:type :invoke, :f :write, :value k})))))

(defn reads
  "We use the last-written atom to perform a read of a randomly selected
  recently written value."
  [last-written]
  (gen/filter (comp complement nil? :value)
              (reify gen/Generator
                (op [this test process]
                  {:type :invoke, :f :read, :value (rand-nth @last-written)}))))

(defn gen
  "Basic generator with n writers, and a buffer of 2n"
  [n]
  (let [last-written (atom
                      (reduce conj clojure.lang.PersistentQueue/EMPTY
                              (repeat (* 2 n) nil)))]
    (gen/reserve n (writes last-written)
                 (reads last-written))))

(defn test
  [opts]
  (let [gen       (gen 10)
        keyrange (atom {})]
    (basic/basic-test
     (merge
      {:name "sequential"
       :key-count 5
       :keyrange  keyrange
       :client {:client (SequentialClient. 10 (atom false) nil)
                :during (gen/stagger 1/100 gen)
                :final nil}
       :checker (checker/compose
                 {:perf (checker/perf)
                  :sequential (checker)})}
      opts))))
