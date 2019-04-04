(ns tidb.sets
  (:refer-clojure :exclude [test])
  (:require [jepsen [client :as client]
                    [checker :as checker]
                    [generator :as gen]]
    [knossos.op :as op]
    [tidb.sql :as c :refer :all]
    [tidb.basic :as basic]
    [clojure.java.jdbc :as j]))

(defrecord SetClient [conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (c/open node)))

  (setup! [this test]
    (j/execute! conn ["create table if not exists sets
                      (id     int not null primary key auto_increment,
                      value bigint not null)"]))

  (invoke! [this test op]
    (with-txn op [c conn]
      (case (:f op)
        :add  (do (j/insert! c :sets (select-keys op [:value]))
                  (assoc op :type :ok))
        :read (->> (j/query c ["select * from sets"])
                   (mapv :value)
                   (into (sorted-set))
                   (assoc op :type :ok, :value)))))

  (teardown! [_ test])

  (close! [_ test]
    (c/close! conn)))

(defn test
  [opts]
  (basic/basic-test
    (merge
    {:name "set"
     :client {:client (SetClient. nil)
              :during (->> (range)
                          (map (partial array-map
                                        :type :invoke
                                        :f :add
                                        :value))
                          gen/seq
                          (gen/stagger 1))
              :final (gen/once {:type :invoke, :f :read, :value nil})}
     :checker (checker/compose
                {:perf (checker/perf)
                 :set  (checker/set)})}
     opts)))
