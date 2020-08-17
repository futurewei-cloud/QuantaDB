(ns jepsen.redis.db
  "Database automation"
  (:require [taoensso.carmine :as car :refer [wcar]]
            [clojure.java [io :as io]
                          [shell :as shell]]
            [clojure [pprint :refer [pprint]]
                     [string :as str]]
            [clojure.tools.logging :refer [info warn]]
            [dom-top.core :refer [with-retry]]
            [jepsen [control :as c]
                    [core :as jepsen]
                    [db :as db]
                    [util :as util :refer [parse-long]]]
            [jepsen.control [net :as cn]
                            [util :as cu]]
            [jepsen.redis [client :as rc]]
            [jepsen.os.debian :as debian]
            [slingshot.slingshot :refer [try+ throw+]]))

(def build-dir
  "A remote directory for us to clone projects and compile them."
  "/tmp/jepsen/build")

(def redis-raft-repo
  "Where can we clone redis-raft from?"
  "https://github.com/RedisLabs/redisraft.git")

(def redis-repo
  "Where can we clone redis from?"
  "https://github.com/redis/redis.git")

(def dir
  "The remote directory where we deploy redis to"
  "/opt/redis")

(def build-file
  "A file we create to track the last built version; speeds up compilation."
  "jepsen-built-version")

(def log-file       (str dir "/redis.log"))
(def pid-file       (str dir "/redis.pid"))
(def binary         "redis-server")
(def cli-binary     "redis-cli")
(def db-file        "redis.rdb")
(def raft-log-file  "raftlog.db")
(def config-file    "redis.conf")

(defn install-build-tools!
  "Installs prerequisite packages for building redis and redisraft."
  []
  (debian/install [:build-essential :cmake :libbsd-dev :libtool :autoconf :automake]))

(defn checkout-repo!
  "Checks out a repo at the given version into a directory in build/ named
  `dir`. Returns the path to the build directory."
  [repo-url dir version]
  (let [full-dir (str build-dir "/" dir)]
    (when-not (cu/exists? full-dir)
      (c/cd build-dir
            (info "Cloning into" full-dir)
            (c/exec :mkdir :-p build-dir)
            (c/exec :git :clone repo-url dir)))

    (c/cd full-dir
          (try+ (c/exec :git :checkout version)
                (catch [:exit 1] e
                  (if (re-find #"pathspec .+ did not match any file" (:err e))
                    (do ; Ah, we're out of date
                        (c/exec :git :fetch)
                        (c/exec :git :checkout version))
                    (throw+ e)))))
    full-dir))

(def build-locks
  "We use these locks to prevent concurrent builds."
  (util/named-locks))

(defmacro with-build-version
  "Takes a test, a repo name, a version, and a body. Builds the repo by
  evaluating body, only if it hasn't already been built. Takes out a lock on a
  per-repo basis to prevent concurrent builds. Remembers what version was last
  built by storing a file in the repo directory. Returns the result of body if
  evaluated, or the build directory."
  [node repo-name version & body]
  `(util/with-named-lock build-locks [~node ~repo-name]
     (let [build-file# (str build-dir "/" ~repo-name "/" build-file)]
       (if (try+ (= (str ~version) (c/exec :cat build-file#))
                (catch [:exit 1] e# ; Not found
                  false))
         ; Already built
         (str build-dir "/" ~repo-name)
         ; Build
         (let [res# (do ~@body)]
           ; Log version
           (c/exec :echo ~version :> build-file#)
           res#)))))

(defn build-redis-raft!
  "Compiles redis-raft from source, and returns the directory we built in."
  [test node]
  (let [version (:raft-version test)]
    (with-build-version node "redis-raft" version
      (let [dir (checkout-repo! redis-raft-repo "redis-raft" version)]
        (info "Building redis-raft" (:raft-version test))
        (c/cd dir
          (c/exec :git :submodule :init)
          (c/exec :git :submodule :update)
          ;(c/exec :make :clean)
          (c/exec :make :cleanall)
          (c/exec :make))
        dir))))

(defn build-redis!
  "Compiles redis, and returns the directory we built in."
  [test node]
  (let [version (:version test)]
    (with-build-version node "redis" version
      (let [dir (checkout-repo! redis-repo "redis" (:version test))]
        (info "Building redis" (:version test))
        (c/cd dir
          (c/exec :make :distclean)
          (c/exec :make))
        dir))))

(defn deploy-redis!
  "Uploads redis binaries built from the given directory."
  [build-dir]
  (info "Deploying redis")
  (c/exec :mkdir :-p dir)
  (doseq [f ["redis-server" "redis-cli"]]
    (c/exec :cp (str build-dir "/src/" f) (str dir "/"))
    (c/exec :chmod "+x" (str dir "/" f))))

(defn deploy-redis-raft!
  "Uploads redis binaries built from the given directory."
  [build-dir]
  (info "Deploying redis-raft")
  (c/exec :mkdir :-p dir)
  (doseq [f ["redisraft.so"]]
    (c/exec :cp (str build-dir "/" f) (str dir "/"))))

(defn cli!
  "Runs a Redis CLI command. Includes a 2s timeout."
  [& args]
  (c/su (apply c/exec :timeout "2s" (str dir "/" cli-binary) args)))

(defn raft-info-str
  "Returns the current cluster state as a string."
  []
  (cli! :--raw "RAFT.INFO"))

(defn parse-raft-info-node
  "Parses a node string in a raft-info string, which is a list of k=v pairs."
  [s]
  (->> (str/split s #",")
       (map (fn parse [part]
              (let [[k v] (str/split part #"=")
                    k (keyword k)]
                [k (case k
                     (:id :last_conn_secs :port :conn_errors :conn_oks)
                     (parse-long v)
                     v)])))
       (into {})))

(defn parse-raft-info-kv
  "Parses a string key and value in a section of the raft info string into a
  key path (for assoc-in) and value [ks v]."
  [section k v]
  (let [k (keyword k)]
    (case section
      :raft     (case k
                  (:role :state) [[section k] (keyword v)]

                  (:node_id :leader_id :current_term :num_nodes)
                  [[section k] (parse-long v)]

                  (if (re-find #"^node(\d+)$" (name k))
                    ; This is a node1, node2, ... entry; file it in :nodes
                    [[section :nodes k] (parse-raft-info-node v)]
                    [[section k] v]))
      :log      [[section k] (parse-long v)]
      :snapshot [[section k] v]
      :clients  (case k
                  :clients_in_multi_state [[section k] (parse-long v)]
                  [[section k] v])
      [[section k] v])))

(defn raft-info
  "Current cluster state as a map."
  []
  (-> (raft-info-str)
      str/split-lines
      (->> (reduce (fn parse-line [[s section] line]
                     (if (re-find #"^\s*$" line)
                       ; Blank
                       [s section]

                       (if-let [m (re-find #"^# (.+)$" line)]
                         ; New section
                         (let [section (keyword (.toLowerCase (m 1)))]
                           [s section])

                         (if-let [[_ k v] (re-find #"^(.+?):(.+)$" line)]
                           ; k:v pair
                           (let [[ks v] (parse-raft-info-kv section k v)]
                             [(assoc-in s ks v) section])

                           ; Don't know
                           (throw+ {:type :raft-info-parse-error
                                    :line line})))))
                   [{} nil]))
       first
       ; Drop node keys; they're not real
       (update-in [:raft :nodes] vals)))

(defn await-node-removal
  "Blocks until node id no longer appears in the local (presumably, leader)'s
  node set."
  [node id]
  (let [r (try+ (raft-info)
                (catch [:exit 1] e
                  :retry)
                (catch Throwable e
                  (warn e "Crash fetching raft-info")
                  :retry))]
    (if (or (= :retry r)
            (= id (:node_id (:raft r)))
            (some #{id} (map :id (:nodes (:raft r)))))
      (do (info :waiting-for-removal id)
          (Thread/sleep 1000)
          (recur node id))
      (do ;(info :done-waiting-for-removal id
                ;(with-out-str (pprint r)))
          :done))))

(def node-ips
  "Returns a map of node names to IP addresses. Memoized."
  (memoize
    (fn node-ips- [test]
      (->> (:nodes test)
           (map (juxt identity cn/ip))
           (into {})))))

(defn node-state
  "This is a bit tricky. Redis-raft lets every node report its own node id, as
  well as the ids and *IP addresses* of other nodes--but the node set doesn't
  include the node you're currently talking to, so we have to combine info from
  multiple parts of the raft-info response. In addition, we use node names,
  rather than IPs, so we have to map those back and forth too.

  At the end of all this, we return a collection of maps, each like

  {:node \"n1\"
   :role :leader
   :id   123}

  This is a best-effort function; when nodes are down we can't get information
  from them. Results may be partial."
  [test]
  (let [ip->node (into {} (map (juxt val key) (node-ips test)))
        states (c/on-nodes test
                           (fn xform [test node]
                             ; We take our local raft info, and massage it
                             ; into a set of {:node n1, :id 123} maps,
                             ; combining info about the local node and
                             ; other nodes.
                             (try+ (let [ri (raft-info)
                                         r  (:raft ri)]
                                     ; Other nodes
                                     (->> (:nodes r)
                                          (map (fn xform-node [n]
                                                 {:id   (:id n)
                                                  :node (ip->node (:addr n))}))
                                          ; Local node
                                          (cons {:id  (:node_id r)
                                                 :role (:role r)
                                                 :node node})))
                                   ; Couldn't run redis-cli
                                   (catch [:exit 1]   e [])
                                   (catch [:exit 124] e []))))]
    ; Now we merge information from all nodes.
    (->> states
         vals
         (apply concat)
         (group-by :node)
         vals
         (map (fn [views-of-node]
                (apply merge views-of-node))))))

(defn node-id
  "Looks up the numeric ID of a node by name."
  [test node]
  (->> (node-state test)
       (filter #(= node (:node %)))
       first
       :id))

(defprotocol Membership
  "Allows a database to support node introspection, growing, and shrinking, the
  cluster. What a mess."
  (get-meta-members [db]       "Local DB membership state machine.")
  (members  [db test]      "The set of nodes currently in the cluster.")
  (join!    [db test node] "Add a node to the cluster.")
  (leave!   [db test node] "Removes a node from the cluster."))

(defprotocol Health
  "Allows a database to signal when a node is alive."
  (up? [db test node]))

(defprotocol Wipe
  "Lets you destroy a database's local state."
  (wipe! [db test node]))

(defn on-some-node
  "Evaluates (f test node) on (randomly ordered) nodes in the test, moving on
  to new nodes when it throws."
  [test f]
  (with-retry [nodes (shuffle (:nodes test))]
    (when (seq nodes)
      (c/on-nodes test (take 1 nodes) f))
    (catch Exception e
      (if-let [more (next nodes)]
        (retry more)
        (throw e)))))

(defn on-some-primary
  "Evaluates (f test node) on some (randomly ordered) primary node in the DB,
  trying each primary in turn until no exception is thrown."
  [db test f]
  (with-retry [nodes (shuffle (db/primaries db test))]
    (when (seq nodes)
      (c/on-nodes test (take 1 nodes) f))
    (catch Exception e
      (if-let [more (next nodes)]
        (retry more)
        (throw e)))))

(defn redis-raft
  "Sets up a Redis-Raft based cluster. Tests should include a :version option,
  which will be the git SHA or tag to build."
  []
  (let [tcpdump (db/tcpdump {:ports [6379]
                             ; HAAACK, this is hardcoded for my cluster control
                             ; node
                             :filter "host 192.168.122.1"})
       ; This atom helps us track which nodes have been removed from the
       ; cluster, when we can delete their data, etc. It'll be lazily
       ; initialized as a part of the setup process, and tracks a map of node
       ; names to membership states. Each membership state is a map like
       ;
       ;  {:state   A keyword for the node state
       ;   :remover A future which is waiting for the node to be removed}
       ;
       ; States are one of
       ;
       ;   :out       - Not in the cluster, data files removed.
       ;   :joining   - We are about to, or are in the process of, joining.
       ;   :live      - Could be in the cluster, at least ostensibly. We enter
       ;                this state before init or join.
       ;   :removing  - We are about to, or have requested, that this node be
       ;                removed. A future will be waiting to clean up its data,
       ;                but won't act until *after* the node is no longer
       ;                present in the removing node's node map.
       meta-members (atom {})]
    (reify db/DB
      (setup! [this test node]
        (when (:tcpdump test) (db/setup! tcpdump test node))
        (c/su
          ; This is a total hack, but since we're grabbing private repos via SSH,
          ; we gotta prime SSH to know about github. Once this repo is public
          ; this nonsense can go away. Definitely not secure, but are we REALLY
          ; being MITMed right now?
          ; (c/exec :ssh-keyscan :-t :rsa "github.com"
          ;        :>> (c/lit "~/.ssh/known_hosts"))

          ; Build and install
          (install-build-tools!)
          (let [redis      (future (-> test (build-redis! node) deploy-redis!))
                redis-raft (future (-> test (build-redis-raft! node)
                                       deploy-redis-raft!))]
            [@redis @redis-raft]

            ; Start
            (db/start! this test node)
            (Thread/sleep 1000) ; TODO: block until port bound

            (if (= node (jepsen/primary test))
              ; Initialize the cluster on the primary
              (do (cli! :raft.cluster :init)
                  (swap! meta-members assoc node {:state :live})
                  (info "Main init done, syncing")
                  (jepsen/synchronize test 600)) ; Compilation can be slow
              ; And join on secondaries.
              (do (info "Waiting for main init")
                  (jepsen/synchronize test 600) ; Ditto
                  (info "Joining")
                  ; Port is mandatory here
                  (swap! meta-members assoc node {:state :live})
                  (cli! :raft.cluster :join (str (jepsen/primary test) ":6379"))))

            (Thread/sleep 2000)
            (info :meta-members meta-members)
            (info :raft-info (raft-info))
            (info :node-state (with-out-str (pprint (node-state test))))
            )))

      (teardown! [this test node]
        ; Welp, time to nuke all our waiting membership threads.
        (doseq [[node {:keys [remover]}] @meta-members]
          (when remover
            (info "Aborting remover thread for" node)
            (future-cancel remover)))

        (db/kill! this test node)
        (c/su (c/exec :rm :-rf dir))
        (when (:tcpdump test)
          (db/teardown! tcpdump test node)))

      db/Primary
      (setup-primary! [_ test node])

      (primaries      [_ test]
        (->> (node-state test)
             (filter (comp #{:leader} :role))
             (map :node)))

      db/Process
      (start! [_ test node]
        (c/su
          (info node :starting :redis)
          (cu/start-daemon!
            {:logfile log-file
             :pidfile pid-file
             :chdir   dir}
            binary
            ; config-file
            :--bind               "0.0.0.0"
            :--dbfilename         db-file
            :--loadmodule         (str dir "/redisraft.so")
            "loglevel=debug"
            (str "raft-log-filename=" raft-log-file)
            (str "raft-log-max-file-size=" (:raft-log-max-file-size test))
            (str "raft-log-max-cache-size=" (:raft-log-max-cache-size test))
            (when (:follower-proxy test) (str "follower-proxy=yes"))
            )))

      (kill! [_ test node]
        (c/su
          (cu/stop-daemon! binary pid-file)))

      db/Pause
      (pause!  [_ test node] (c/su (cu/grepkill! :stop binary)))
      (resume! [_ test node] (c/su (cu/grepkill! :cont binary)))

      Health
      (up? [db test node]
        (try (let [conn (rc/open node {:timeout-ms 1000})]
               (try (= "PONG" (wcar conn (car/ping)))
                    (finally (rc/close! conn))))
             (catch java.net.SocketTimeoutException e
               false) ; Probably?
             (catch java.net.ConnectException e
               false)))

      Membership
      (get-meta-members [db] @meta-members)

      (members [db test]
        ; We take the self-reported node states...
        (->> (node-state test)
             (map :node)
             ; But because redis nodes stay up and think they're candidates
             ; after being removed, we explicitly filter out dead/removing
             ; nodes. Note that we leave :removing nodes in here, because
             ; we might have started their removal process, but don't actually
             ; know they're removed--or even that they will be removed. The
             ; remove call might have failed, so we might need to try it again.
             (remove (->> @meta-members
                          (filter (comp #{:dead} :state val))
                          (map key)
                          set))))

      (join! [db test node]
        (let [up (filter (partial up? db test) (members db test))
              _  (when (empty? up)
                   ; We can't actually join to anyone right now. This isn't
                   ; bulletproof, but it really cuts down on the number of
                   ; crashes.
                   (throw+ {:type :no-up-node-to-join-to
                            :node node}))
              target (rand-nth up)
              ; OK, we've got a node to join to. Can we join?
              m (swap! meta-members update-in [node :state]
                       {:dead     :joining      ; We can join a dead node
                        :joining  :joining      ; We can try to join again
                        :live     :live         ; But we can't join a live node
                        :removing :removing})]  ; Or one being removed

          ; Are we joining? If not, abort here.
          (when-not (= :joining (get-in m [node :state]))
            (throw+ {:type    :can't-join-in-this-state
                     :node    node
                     :members m}))

          ; Good, let's go.
          (let [res (c/on-nodes test [node] (fn start+join [_ _]
                                              (db/start! db test node)
                                              (Thread/sleep 1000)
                                              (info node :joining target)
                                              (cli! :raft.cluster :join
                                                    (str target ":6379"))))]
            ; And mark that the join completed.
            (swap! meta-members assoc-in [node :state] :live)
            res)))

      (leave! [db test node-or-map]
        (let [[node primary] (if (map? node-or-map)
                               [(:remove node-or-map) (:using node-or-map)]
                               [node-or-map nil])
              id  (node-id test node)
              ; Spawns a future which waits for the node to actually finish
              ; being removed, optionally nukes the data, and transitions the
              ; member to the :dead state.
              remover (fn [local]
                        (future
                          (await-node-removal node id)
                          (info local :removed node (str "(id: " id ")"))
                          (when (:nuke-after-leave test)
                            ; Give em a bit to, you know, screw stuff up. Maybe
                            ; answer some requests with stale data, or execute
                            ; writes.
                            (Thread/sleep 10000)
                            (c/on-nodes test [node]
                                        (fn [_ _]
                                          (info "Killing and wiping" node)
                                          (db/kill! db test node)
                                          (wipe! db test node))))
                          ; Update members to clean up this future and mark us
                          ; as dead.
                          (swap! meta-members (fn [m]
                                           (-> m
                                               (update node dissoc :remover)
                                               (update node assoc :state :dead))))))
              ; Evaluated on a primary, puts us into the leaving state, spawns
              ; the future to complete the remove process, and asks the node to
              ; be removed.
              leave!
              (fn leave! [test local]
                (info local :removing node
                      (str "(id: " id ")"))
                ; First up, update our state. We can only remove if
                ; we're alive or already removing.
                (let [m (swap! meta-members update-in [node :state]
                               {:dead      :dead
                                :joining   :joining
                                :live      :removing
                                :removing  :removing})]

                  ; Make sure we're able to remove
                  (when-not (= :removing (get-in m [node :state]))
                    (throw+ {:type  :can't-remove-in-this-state
                             :node  node
                             :members m}))

                  ; OK, this is a mess. Our call to REMOVE the node might
                  ; fail--and if it does, we don't necessarily know whether the
                  ; node is going to be removed or not. If we spawn this
                  ; cleanup future before calling, then we might wind up nuking
                  ; a node which is still supposed to be in the cluster--maybe,
                  ; for instance, it's joining and this primary doesn't know
                  ; about it yet. On the flip side, if we spawn the cleanup
                  ; future after calling REMOVE, we might fail to kill and wipe
                  ; a node which actually WILL be removed later, and get the
                  ; node stuck in the :removing state indefinitely. I think
                  ; that's safer, so that's what we're doing, but ugh... this
                  ; whole thing is a fragile mess.

                  ; OK, ask to remove the node. This should yield "OK".
                  ;
                  ; One of (many) weird things that could happen here: the node
                  ; we're removing could still be joining, and unknown to this
                  ; primary, but we have it in the :live state because we
                  ; successfully completed the join call. This call will fail,
                  ; and we'll be left with a "live" node in the :removing
                  ; state. This is OK, because :removing doesn't... ACTUALLY
                  ; mean removing--we actually WANT to come back and try to
                  ; remove it again later.
                  ;
                  ; Sigh.
                  (let [res (cli! "RAFT.NODE" "REMOVE" id)]
                    ; (info local :remove node id :returned res)
                    (when (= "OK" res)
                      ; OK, we're gonna remove... eventually. Start the cleanup
                      ; future.
                      (locking meta-members
                        (when-not (get-in @meta-members [node :remover])
                          (swap! meta-members assoc-in [node :remover]
                                 (remover local)))))
                    res)))

              ; Depending on whether we were asked to remove on a specific node
              ; or not, go ahead and try to leave.
              res (if primary
                    (c/on-nodes test [primary] leave!)
                    (on-some-primary db test leave!))]
          (or res :no-primary-available)))

          Wipe
          (wipe! [db test node]
                 (info "Wiping node")
                 (c/su
                   (c/cd dir
                         (c/exec :rm :-f db-file raft-log-file))))

          db/LogFiles
          (log-files [_ test node]
                     (concat [log-file
                              (str dir "/" db-file)
                              (str dir "/" raft-log-file)]
                             (when (:tcpdump test)
                               (db/log-files tcpdump test node)))))))

(def crash-pattern
  "An egrep pattern we use to find crashes in the redis logs."
  "panic|assert|bug report|stack trace")

(defn logged-crashes
  "Takes a test, and returns a map of nodes to strings from their redis logs
  that look like crashes, or nil if no crashes occurred."
  ([test]
   (let [crashes (->> (c/on-many (:nodes test)
                                 (try+
                                   (c/exec :egrep :-i crash-pattern log-file)
                                   (catch [:type :jepsen.control/nonzero-exit] e
                                     nil)))
                      (keep (fn [[k v :as pair]]
                              (when v pair))))]
     (when (seq crashes)
       (into {} crashes)))))
