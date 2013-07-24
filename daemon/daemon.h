#ifndef DAEMON_H
#define DAEMON_H

#include <map>
#include <string>

#include "clients.h"

class Client;
class ConfCSMsg;
class DiscoverSched;
class GetNativeEnvMsg;
class JobDoneMsg;
class Msg;
class MsgChannel;
class VerifyEnvMsg;


using namespace std;

struct NativeEnvironment {
    string name; // the hash
    map<string, time_t> extrafilestimes;
    // Timestamps for compiler binaries, if they have changed since the time
    // the native env was built, it needs to be rebuilt.
    time_t gcc_bin_timestamp;
    time_t gpp_bin_timestamp;
    time_t clang_bin_timestamp;
};

struct Daemon {

public:
    Daemon();

    bool reannounce_environments() __attribute_warn_unused_result__;
    int answer_client_requests();
    bool handle_transfer_env(Client *client, Msg *msg) __attribute_warn_unused_result__;
    bool handle_transfer_env_done(Client *client);
    bool handle_get_native_env(Client *client, GetNativeEnvMsg *msg) __attribute_warn_unused_result__;
    void handle_old_request();
    bool handle_compile_file(Client *client, Msg *msg) __attribute_warn_unused_result__;
    bool handle_activity(Client *client) __attribute_warn_unused_result__;
    bool handle_file_chunk_env(Client *client, Msg *msg) __attribute_warn_unused_result__;
    void handle_end(Client *client, int exitcode);
    int scheduler_get_internals() __attribute_warn_unused_result__;
    void clear_children();
    int scheduler_use_cs(UseCSMsg *msg) __attribute_warn_unused_result__;
    bool handle_get_cs(Client *client, Msg *msg) __attribute_warn_unused_result__;
    bool handle_local_job(Client *client, Msg *msg) __attribute_warn_unused_result__;
    bool handle_job_done(Client *cl, JobDoneMsg *m) __attribute_warn_unused_result__;
    bool handle_compile_done(Client *client) __attribute_warn_unused_result__;
    bool handle_verify_env(Client *client, VerifyEnvMsg *msg) __attribute_warn_unused_result__;
    bool handle_blacklist_host_env(Client *client, Msg *msg) __attribute_warn_unused_result__;
    int handle_cs_conf(ConfCSMsg *msg);
    string dump_internals() const;
    string determine_nodename();
    void determine_system();
    bool maybe_stats(bool force = false);
    bool send_scheduler(const Msg &msg) __attribute_warn_unused_result__;
    void close_scheduler();
    bool reconnect();
    int working_loop();
    bool setup_listen_fds();
    void check_cache_size(const string &new_env);

private:
    string m_envBaseDir;
    int m_tcpListenFd;
    int m_unixListenFd;
    int m_newClientId;
    time_t m_nextSchedulerConnect;
    size_t m_cacheSize;
    bool m_noRemote;
    bool m_customNodeName;
    unsigned long m_icecreamLoad;
    struct timeval m_icecreamUsage;
    int m_currentLoad;
    int m_numCpus;
    MsgChannel *m_scheduler;
    DiscoverSched *m_discover;
    int m_maxSchedulerPong;
    int m_maxSchedulerPing;
    string m_benchSource;
    unsigned int m_currentKids;
    Clients m_clients;
    map<string, time_t> m_envsLastUse;
    // Map of native environments, the basic one(s) containing just the compiler
    // and possibly more containing additional files (such as compiler plugins).
    // The key is the compiler name and a concatenated list of the additional files
    // (or just the compiler name for the basic ones).
    map<string, NativeEnvironment> m_nativeEnvironments;
    uid_t m_uid;
    gid_t m_gid;
    bool m_warnIceccUser;
    string m_machineName;
    string m_nodeName;
    map<int, MsgChannel *> m_fd2chan;
    string m_remoteName;
    string m_netName;
    string m_schedulerName;
};

#endif
