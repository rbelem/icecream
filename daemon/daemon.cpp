/* -*- mode: C++; indent-tabs-mode: nil; c-basic-offset: 4; fill-column: 99; -*- */
/* vim: set ts=4 sw=4 et tw=99:  */
/*
    This file is part of Icecream.

    Copyright (c) 2004 Stephan Kulow <coolo@suse.de>
                  2002, 2003 by Martin Pool <mbp@samba.org>

    This program is free software; you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation; either version 2 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License along
    with this program; if not, write to the Free Software Foundation, Inc.,
    51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
*/

#include "daemon.h"

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <fcntl.h>
#include <errno.h>
#include <netdb.h>
#include <getopt.h>

#ifdef HAVE_SIGNAL_H
#include <signal.h>
#endif
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/un.h>
#include <sys/param.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <pwd.h>

#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/utsname.h>

#ifdef HAVE_ARPA_NAMESER_H
#  include <arpa/nameser.h>
#endif

#ifdef HAVE_SYS_VFS_H
#include <sys/vfs.h>
#endif

#include <arpa/inet.h>

#ifdef HAVE_RESOLV_H
#  include <resolv.h>
#endif
#include <netdb.h>

#ifdef HAVE_SYS_RESOURCE_H
#  include <sys/resource.h>
#endif

#ifndef RUSAGE_SELF
#  define RUSAGE_SELF (0)
#endif
#ifndef RUSAGE_CHILDREN
#  define RUSAGE_CHILDREN (-1)
#endif

#ifdef HAVE_LIBCAP_NG
#  include <cap-ng.h>
#endif

#include <deque>
#include <map>
#include <algorithm>
#include <set>
#include <fstream>
#include <string>

#include "ncpus.h"
#include "exitcode.h"
#include "serve.h"
#include "workit.h"
#include "logging.h"
#include <comm.h>
#include "load.h"
#include "environment.h"
#include "platform.h"

const int PORT = 10245;
static std::string pidFilePath;

#ifndef __attribute_warn_unused_result__
#define __attribute_warn_unused_result__
#endif

using namespace std;

struct timeval last_stat;
int mem_limit = 100;
unsigned int max_kids = 0;

size_t cache_size_limit = 100 * 1024 * 1024;

Daemon::Daemon()
    : m_envBaseDir("/tmp/icecc-envs")
    , m_tcpListenFd(-1)
    , m_unixListenFd(-1)
    , m_newClientId(0)
    , m_nextSchedulerConnect(0)
    , m_cacheSize(0)
    , m_noRemote(false)
    , m_customNodeName(false)
    , m_icecreamLoad(0)
    , m_icecreamUsage()
    , m_currentLoad(-1000)
    , m_numCpus(0)
    , m_scheduler(0)
    , m_discover(0)
    , m_maxSchedulerPong(MAX_SCHEDULER_PONG)
    , m_maxSchedulerPing(MAX_SCHEDULER_PING)
    , m_benchSource("")
    , m_currentKids(0)
    , m_clients()
    , m_nativeEnvironments()
    , m_uid()
    , m_gid()
    , m_warnIceccUser()
    , m_machineName()
    , m_nodeName()
    , m_fd2chan()
    , m_remoteName()
    , m_netName()
    , m_schedulerName()
{
    m_icecreamUsage.tv_sec = m_icecreamUsage.tv_usec = 0;

    if (getuid() == 0) {
        struct passwd *pw = getpwnam("icecc");

        if (pw) {
            m_uid = pw->pw_uid;
            m_gid = pw->pw_gid;
            m_warnIceccUser = false;
        } else {
            m_warnIceccUser = true;
            m_uid = 65534;
            m_gid = 65533;
        }
    } else {
        m_uid = getuid();
        m_gid = getgid();
    }

}

bool Daemon::setup_listen_fds()
{
    m_tcpListenFd = -1;

    if (!m_noRemote) { // if we only listen to local clients, there is no point in going TCP
        if ((m_tcpListenFd = socket(PF_INET, SOCK_STREAM, 0)) < 0) {
            log_perror("socket()");
            return false;
        }

        int optval = 1;

        if (setsockopt(m_tcpListenFd, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval)) < 0) {
            log_perror("setsockopt()");
            return false;
        }

        int count = 5;

        while (count) {
            struct sockaddr_in myaddr;
            myaddr.sin_family = AF_INET;
            myaddr.sin_port = htons(PORT);
            myaddr.sin_addr.s_addr = INADDR_ANY;

            if (bind(m_tcpListenFd, (struct sockaddr *)&myaddr,
                     sizeof(myaddr)) < 0) {
                log_perror("bind()");
                sleep(2);

                if (!--count) {
                    return false;
                }

                continue;
            } else {
                break;
            }
        }

        if (listen(m_tcpListenFd, 20) < 0) {
            log_perror("listen()");
            return false;
        }

        fcntl(m_tcpListenFd, F_SETFD, FD_CLOEXEC);
    }

    if ((m_unixListenFd = socket(AF_UNIX, SOCK_STREAM, 0)) < 0) {
        log_perror("socket()");
        return false;
    }

    struct sockaddr_un myaddr;

    memset(&myaddr, 0, sizeof(myaddr));

    myaddr.sun_family = AF_UNIX;

    mode_t old_umask = -1U;

#ifdef HAVE_LIBCAP_NG
    // We run as system daemon.
    if (capng_have_capability( CAPNG_PERMITTED, CAP_SYS_CHROOT )) {
#else
    if (getuid() == 0) {
#endif
        strncpy(myaddr.sun_path, "/var/run/icecc/iceccd.socket", sizeof(myaddr.sun_path) - 1);
        unlink(myaddr.sun_path);
        old_umask = umask(0);
    } else { // Started by user.
        if( getenv( "HOME" )) {
            strncpy(myaddr.sun_path, getenv("HOME"), sizeof(myaddr.sun_path) - 1);
            strncat(myaddr.sun_path, "/.iceccd.socket", sizeof(myaddr.sun_path) - 1 - strlen(myaddr.sun_path));
            unlink(myaddr.sun_path);
        } else {
            log_error() << "launched by user, but $HOME not set" << endl;
            return false;
        }
    }

    if (bind(m_unixListenFd, (struct sockaddr*)&myaddr, sizeof(myaddr)) < 0) {
        log_perror("bind()");

        if (old_umask != -1U) {
            umask(old_umask);
        }

        return false;
    }

    if (old_umask != -1U) {
        umask(old_umask);
    }

    if (listen(m_unixListenFd, 20) < 0) {
        log_perror("listen()");
        return false;
    }

    fcntl(m_unixListenFd, F_SETFD, FD_CLOEXEC);

    return true;
}

void Daemon::determine_system()
{
    struct utsname uname_buf;

    if (uname(&uname_buf)) {
        log_perror("uname call failed");
        return;
    }

    if (m_nodeName.length() && (m_nodeName != uname_buf.nodename)) {
        m_customNodeName  = true;
    }

    if (!m_customNodeName) {
        m_nodeName = uname_buf.nodename;
    }

    m_machineName = determine_platform();
}

string Daemon::determine_nodename()
{
    if (m_customNodeName && !m_nodeName.empty()) {
        return m_nodeName;
    }

    // perhaps our host name changed due to network change?
    struct utsname uname_buf;

    if (!uname(&uname_buf)) {
        m_nodeName = uname_buf.nodename;
    }

    return m_nodeName;
}

bool Daemon::send_scheduler(const Msg& msg)
{
    if (!m_scheduler) {
        log_error() << "scheduler dead ?!" << endl;
        return false;
    }

    if (!m_scheduler->send_msg(msg)) {
        log_error() << "sending to scheduler failed.." << endl;
        close_scheduler();
        return false;
    }

    return true;
}

bool Daemon::reannounce_environments()
{
    log_error() << "reannounce_environments " << endl;
    LoginMsg lmsg(0, m_nodeName, "");
    lmsg.envs = available_environmnents(m_envBaseDir);
    return send_scheduler(lmsg);
}

void Daemon::close_scheduler()
{
    if (!m_scheduler) {
        return;
    }

    delete m_scheduler;
    m_scheduler = 0;
    delete m_discover;
    m_discover = 0;
    m_nextSchedulerConnect = time(0) + 20 + (rand() & 31);
}

bool Daemon::maybe_stats(bool send_ping)
{
    struct timeval now;
    gettimeofday(&now, 0);

    time_t diff_sent = (now.tv_sec - last_stat.tv_sec) * 1000 + (now.tv_usec - last_stat.tv_usec) / 1000;

    if (diff_sent >= m_maxSchedulerPong * 1000) {
        StatsMsg msg;
        unsigned int memory_fillgrade;
        unsigned long idleLoad = 0;
        unsigned long niceLoad = 0;

        if (!fill_stats(idleLoad, niceLoad, memory_fillgrade, &msg, m_clients.activeProcesses())) {
            return false;
        }

        time_t diff_stat = (now.tv_sec - last_stat.tv_sec) * 1000 + (now.tv_usec - last_stat.tv_usec) / 1000;
        last_stat = now;

        /* m_icecreamLoad contains time in milliseconds we have used for icecream */
        /* idle time could have been used for icecream, so claim it */
        m_icecreamLoad += idleLoad * diff_stat / 1000;

        /* add the time of our childrens, but only the time since the last run */
        struct rusage ru;

        if (!getrusage(RUSAGE_CHILDREN, &ru)) {
            uint32_t ice_msec = ((ru.ru_utime.tv_sec - m_icecreamUsage.tv_sec) * 1000
                                 + (ru.ru_utime.tv_usec - m_icecreamUsage.tv_usec) / 1000) / m_numCpus;

            /* heuristics when no child terminated yet: account 25% of total nice as our clients */
            if (!ice_msec && m_currentKids) {
                ice_msec = (niceLoad * diff_stat) / (4 * 1000);
            }

            m_icecreamLoad += ice_msec * diff_stat / 1000;

            m_icecreamUsage.tv_sec = ru.ru_utime.tv_sec;
            m_icecreamUsage.tv_usec = ru.ru_utime.tv_usec;
        }

        int idle_average = m_icecreamLoad;

        if (diff_sent) {
            idle_average = m_icecreamLoad * 1000 / diff_sent;
        }

        if (idle_average > 1000) {
            idle_average = 1000;
        }

        msg.load = ((700 * (1000 - idle_average)) + (300 * memory_fillgrade)) / 1000;

        if (memory_fillgrade > 600) {
            msg.load = 1000;
        }

        if (idle_average < 100) {
            msg.load = 1000;
        }

#ifdef HAVE_SYS_VFS_H
        struct statfs buf;
        int ret = statfs(m_envBaseDir.c_str(), &buf);

        if (!ret && long(buf.f_bavail) < ((long(max_kids + 1 - m_currentKids) * 4 * 1024 * 1024) / buf.f_bsize)) {
            msg.load = 1000;
        }

#endif

        // Matz got in the urine that not all CPUs are always feed
        mem_limit = std::max(int(msg.freeMem / std::min(std::max(max_kids, 1U), 4U)), int(100U));

        if (abs(int(msg.load) - m_currentLoad) >= 100 || send_ping) {
            if (!send_scheduler(msg)) {
                return false;
            }
        }

        m_icecreamLoad = 0;
        m_currentLoad = msg.load;
    }

    return true;
}

string Daemon::dump_internals() const
{
    string result;

    result += "Node Name: " + m_nodeName + "\n";
    result += "  Remote name: " + m_remoteName + "\n";

    for (map<int, MsgChannel *>::const_iterator it = m_fd2chan.begin(); it != m_fd2chan.end(); ++it)  {
        result += "  m_fd2chan[" + toString(it->first) + "] = " + it->second->dump() + "\n";
    }

    for (Clients::const_iterator it = m_clients.begin(); it != m_clients.end(); ++it)  {
        result += "  client " + toString(it->second->clientId()) + ": " + it->second->dump() + "\n";
    }

    if (m_cacheSize) {
        result += "  Cache Size: " + toString(m_cacheSize) + "\n";
    }

    result += "  Architecture: " + m_machineName + "\n";

    for (map<string, NativeEnvironment>::const_iterator it = m_nativeEnvironments.begin();
            it != m_nativeEnvironments.end(); ++it) {
        result += "  NativeEnv (" + it->first + "): " + it->second.name + "\n";
    }

    if (!m_envsLastUse.empty()) {
        result += "  Now: " + toString(time(0)) + "\n";
    }

    for (map<string, time_t>::const_iterator it = m_envsLastUse.begin();
            it != m_envsLastUse.end(); ++it)  {
        result += "  m_envsLastUse[" + it->first  + "] = " + toString(it->second) + "\n";
    }

    result += "  Current kids: " + toString(m_currentKids) + " (max: " + toString(max_kids) + ")\n";

    if (m_scheduler) {
        result += "  Scheduler protocol: " + toString(m_scheduler->protocol) + "\n";
    }

    StatsMsg msg;
    unsigned int memory_fillgrade = 0;
    unsigned long idleLoad = 0;
    unsigned long niceLoad = 0;

    if (fill_stats(idleLoad, niceLoad, memory_fillgrade, &msg, m_clients.activeProcesses())) {
        result += "  cpu: " + toString(idleLoad) + " idle, "
                  + toString(niceLoad) + " nice\n";
        result += "  load: " + toString(msg.loadAvg1 / 1000.) + ", m_icecreamLoad: "
                  + toString(m_icecreamLoad) + "\n";
        result += "  memory: " + toString(memory_fillgrade)
                  + " (free: " + toString(msg.freeMem) + ")\n";
    }

    return result;
}

int Daemon::scheduler_get_internals()
{
    trace() << "handle_get_internals " << dump_internals() << endl;
    return send_scheduler(StatusTextMsg(dump_internals())) ? 0 : 1;
}

int Daemon::scheduler_use_cs(UseCSMsg *msg)
{
    Client *c = m_clients.find_by_client_id(msg->client_id);
    trace() << "handle_use_cs " << msg->job_id << " " << msg->client_id
            << " " << c << " " << msg->hostname << " " << m_remoteName <<  endl;

    if (!c) {
        if (send_scheduler(JobDoneMsg(msg->job_id, 107, JobDoneMsg::FROM_SUBMITTER))) {
            return 1;
        }

        return 1;
    }

    if (msg->hostname == m_remoteName) {
        c->setUseCSMsg(new UseCSMsg(msg->host_platform, "127.0.0.1", PORT, msg->job_id, true, 1,
                                    msg->matched_job_id));
        c->setStatus(Client::PENDING_USE_CS);
    } else {
        c->setUseCSMsg(new UseCSMsg(msg->host_platform, msg->hostname, msg->port,
                                    msg->job_id, true, 1, msg->matched_job_id));

        if (!c->channel()->send_msg(*msg)) {
            handle_end(c, 143);
            return 0;
        }

        c->setStatus(Client::WAITCOMPILE);
    }

    c->setJobId(msg->job_id);

    return 0;
}

bool Daemon::handle_transfer_env(Client *client, Msg *_msg)
{
    log_error() << "handle_transfer_env" << endl;

    assert(client->status() != Client::TOINSTALL
            && client->status() != Client::TOCOMPILE
            && client->status() != Client::WAITCOMPILE);
    assert(client->pipeToChild() < 0);

    EnvTransferMsg *emsg = static_cast<EnvTransferMsg *>(_msg);
    string target = emsg->target;

    if (target.empty()) {
        target =  m_machineName;
    }

    int sock_to_stdin = -1;
    FileChunkMsg *fmsg = 0;

    pid_t pid = start_install_environment(m_envBaseDir, target, emsg->name, client->channel(),
                                          sock_to_stdin, fmsg, m_uid, m_gid);

    client->setStatus(Client::TOINSTALL);
    client->setOutFile(emsg->target + "/" + emsg->name);

    if (pid > 0) {
        log_error() << "got pid " << pid << endl;
        m_currentKids++;
        client->setPipeToChild(sock_to_stdin);
        client->setChildPid(pid);

        if (!handle_file_chunk_env(client, fmsg)) {
            pid = 0;
        }
    }

    if (pid <= 0) {
        handle_transfer_env_done(client);
    }

    delete fmsg;
    return pid > 0;
}

bool Daemon::handle_transfer_env_done(Client *client)
{
    log_error() << "handle_transfer_env_done" << endl;

    assert(client->outFile().size());
    assert(client->status() == Client::TOINSTALL);

    size_t installed_size = finalize_install_environment(m_envBaseDir, client->outFile(),
                            client->childPid(), m_uid, m_gid);

    if (client->pipeToChild() >= 0) {
        installed_size = 0;
        close(client->pipeToChild());
        client->setPipeToChild(-1);
    }

    client->setStatus(Client::UNKNOWN);
    string current = client->outFile();
    client->outFile().clear();
    client->setChildPid(-1);
    assert(m_currentKids > 0);
    m_currentKids--;

    log_error() << "installed_size: " << installed_size << endl;

    if (installed_size) {
        m_cacheSize += installed_size;
        m_envsLastUse[current] = time(NULL);
        log_error() << "installed " << current << " size: " << installed_size
                    << " all: " << m_cacheSize << endl;
    }

    check_cache_size(current);

    bool r = reannounce_environments(); // do that before the file compiles

    // we do that here so we're not given out in case of full discs
    if (!maybe_stats(true)) {
        r = false;
    }

    return r;
}

void Daemon::check_cache_size(const string &new_env)
{
    time_t now = time(NULL);

    while (m_cacheSize > cache_size_limit) {
        string oldest;
        // I don't dare to use (time_t)-1
        time_t oldest_time = time(NULL) + 90000;
        bool oldest_is_native = false;

        for (map<string, time_t>::const_iterator it = m_envsLastUse.begin();
                it != m_envsLastUse.end(); ++it) {
            trace() << "das ist jetzt so: " << it->first << " " << it->second << " " << oldest_time << endl;
            // ignore recently used envs (they might be in use _right_ now)
            int keep_timeout = 200;
            bool native = false;

            // If it is a native environment, allow removing it only after a longer period,
            // unless there are many native environments.
            for (map<string, NativeEnvironment>::const_iterator it2 = m_nativeEnvironments.begin();
                    it2 != m_nativeEnvironments.end(); ++it2) {
                if (it2->second.name == it->first) {
                    native = true;

                    if (m_nativeEnvironments.size() < 5) {
                        keep_timeout = 24 * 60 * 60;    // 1 day
                    }

                    break;
                }
            }

            if (it->second < oldest_time && now - it->second > keep_timeout) {
                bool env_currently_in_use = false;

                for (Clients::const_iterator it2 = m_clients.begin(); it2 != m_clients.end(); ++it2)  {
                    if (it2->second->status() == Client::TOCOMPILE
                            || it2->second->status() == Client::TOINSTALL
                            || it2->second->status() == Client::WAITFORCHILD) {

                        assert(it2->second->job());
                        string envforjob = it2->second->job()->targetPlatform() + "/"
                                           + it2->second->job()->environmentVersion();

                        if (envforjob == it->first) {
                            env_currently_in_use = true;
                        }
                    }
                }

                if (!env_currently_in_use) {
                    oldest_time = it->second;
                    oldest = it->first;
                    oldest_is_native = native;
                }
            }
        }

        if (oldest.empty() || oldest == new_env) {
            break;
        }

        size_t removed;

        if (oldest_is_native) {
            removed = remove_native_environment(oldest);
            trace() << "removing " << oldest << " " << oldest_time << " " << removed << endl;
        } else {
            removed = remove_environment(m_envBaseDir, oldest);
            trace() << "removing " << m_envBaseDir << "/" << oldest << " " << oldest_time
                    << " " << removed << endl;
        }

        m_cacheSize -= min(removed, m_cacheSize);
        m_envsLastUse.erase(oldest);
    }
}

bool Daemon::handle_get_native_env(Client *client, GetNativeEnvMsg *msg)
{
    string env_key;
    map<string, time_t> extrafilestimes;
    env_key = msg->compiler;

    for (list<string>::const_iterator it = msg->extrafiles.begin();
            it != msg->extrafiles.end(); ++it) {
        env_key += ':';
        env_key += *it;
        struct stat st;

        if (stat(it->c_str(), &st) != 0) {
            trace() << "Extra file " << *it << " for environment not found." << endl;
            client->channel()->send_msg(EndMsg());
            handle_end(client, 122);
            return false;
        }

        extrafilestimes[*it] = st.st_mtime;
    }

    if (m_nativeEnvironments[env_key].name.length()) {
        const NativeEnvironment &env = m_nativeEnvironments[env_key];

        if (!compilers_uptodate(env.gcc_bin_timestamp, env.gpp_bin_timestamp, env.clang_bin_timestamp)
                || env.extrafilestimes != extrafilestimes
                || access(env.name.c_str(), R_OK) != 0) {
            trace() << "native_env needs rebuild" << endl;
            m_cacheSize -= remove_native_environment(env.name);
            m_envsLastUse.erase(env.name);
            m_nativeEnvironments.erase(env_key);   // invalidates 'env'
        }
    }

    trace() << "get_native_env " << m_nativeEnvironments[env_key].name
            << " (" << env_key << ")" << endl;

    if (!m_nativeEnvironments[env_key].name.length()) {
        NativeEnvironment &env = m_nativeEnvironments[env_key]; // also inserts it
        size_t installed_size = setup_env_cache(m_envBaseDir, env.name,
                                                m_uid, m_gid, msg->compiler, msg->extrafiles);
        // we only clean out cache on next target install
        m_cacheSize += installed_size;
        trace() << "m_cacheSize = " << m_cacheSize << endl;

        if (!installed_size) {
            client->channel()->send_msg(EndMsg());
            handle_end(client, 121);
            return false;
        }

        env.extrafilestimes = extrafilestimes;
        save_compiler_timestamps(env.gcc_bin_timestamp, env.gpp_bin_timestamp, env.clang_bin_timestamp);
        m_envsLastUse[m_nativeEnvironments[env_key].name] = time(NULL);
        check_cache_size(env.name);
    }

    UseNativeEnvMsg m(m_nativeEnvironments[env_key].name);

    if (!client->channel()->send_msg(m)) {
        handle_end(client, 138);
        return false;
    }

    m_envsLastUse[m_nativeEnvironments[env_key].name] = time(NULL);
    client->setStatus(Client::GOTNATIVE);
    return true;
}

bool Daemon::handle_job_done(Client *cl, JobDoneMsg *m)
{
    if (cl->status() == Client::CLIENTWORK) {
        m_clients.setActiveProcesses(m_clients.activeProcesses() - 1);
    }

    cl->setStatus(Client::JOBDONE);
    JobDoneMsg *msg = static_cast<JobDoneMsg *>(m);
    trace() << "handle_job_done " << msg->job_id << " " << msg->exitcode << endl;

    if (!m->is_from_server()
            && (m->user_msec + m->sys_msec) <= m->real_msec) {
        m_icecreamLoad += (m->user_msec + m->sys_msec) / m_numCpus;
    }

    assert(msg->job_id == cl->jobId());
    cl->setJobId(0); // the scheduler doesn't have it anymore
    return send_scheduler(*msg);
}

void Daemon::handle_old_request()
{
    while ((m_currentKids + m_clients.activeProcesses()) < max_kids) {

        Client *client = m_clients.get_earliest_client(Client::LINKJOB);

        if (client) {
            trace() << "send JobLocalBeginMsg to client" << endl;

            if (!client->channel()->send_msg(JobLocalBeginMsg())) {
                log_warning() << "can't send start message to client" << endl;
                handle_end(client, 112);
            } else {
                client->setStatus(Client::CLIENTWORK);
                m_clients.setActiveProcesses(m_clients.activeProcesses() + 1);
                trace() << "pushed local job " << client->clientId() << endl;

                if (!send_scheduler(JobLocalBeginMsg(client->clientId(), client->outFile()))) {
                    return;
                }
            }

            continue;
        }

        client = m_clients.get_earliest_client(Client::PENDING_USE_CS);

        if (client) {
            trace() << "pending " << client->dump() << endl;

            if (client->channel()->send_msg(*client->useCSMsg())) {
                client->setStatus(Client::CLIENTWORK);
                /* we make sure we reserve a spot and the rest is done if the
                 * client contacts as back with a Compile request */
                m_clients.setActiveProcesses(m_clients.activeProcesses() + 1);
            } else {
                handle_end(client, 129);
            }

            continue;
        }

        /* we don't want to handle TOCOMPILE jobs as long as our load
           is too high */
        if (m_currentLoad >= 1000) {
            break;
        }

        client = m_clients.get_earliest_client(Client::TOCOMPILE);

        if (client) {
            CompileJob *job = client->job();
            assert(job);
            int sock = -1;
            pid_t pid = -1;

            trace() << "requests--" << job->jobID() << endl;

            string envforjob = job->targetPlatform() + "/" + job->environmentVersion();
            m_envsLastUse[envforjob] = time(NULL);
            pid = handle_connection(m_envBaseDir, job, client->channel(), sock, mem_limit, m_uid, m_gid);
            trace() << "handle connection returned " << pid << endl;

            if (pid > 0) {
                m_currentKids++;
                client->setStatus(Client::WAITFORCHILD);
                client->setPipeToChild(sock);
                client->setChildPid(pid);

                if (!send_scheduler(JobBeginMsg(job->jobID()))) {
                    log_info() << "failed sending scheduler about " << job->jobID() << endl;
                }
            } else {
                handle_end(client, 117);
            }

            continue;
        }

        break;
    }
}

bool Daemon::handle_compile_done(Client *client)
{
    assert(client->status() == Client::WAITFORCHILD);
    assert(client->childPid() > 0);
    assert(client->pipeToChild() >= 0);

    JobDoneMsg *msg = new JobDoneMsg(client->job()->jobID(), -1, JobDoneMsg::FROM_SERVER);
    assert(msg);
    assert(m_currentKids > 0);
    m_currentKids--;

    unsigned int job_stat[8];
    int end_status = 151;

    if (read(client->pipeToChild(), job_stat, sizeof(job_stat)) == sizeof(job_stat)) {
        msg->in_uncompressed = job_stat[JobStatistics::in_uncompressed];
        msg->in_compressed = job_stat[JobStatistics::in_compressed];
        msg->out_compressed = msg->out_uncompressed = job_stat[JobStatistics::out_uncompressed];
        end_status = msg->exitcode = job_stat[JobStatistics::exit_code];
        msg->real_msec = job_stat[JobStatistics::real_msec];
        msg->user_msec = job_stat[JobStatistics::user_msec];
        msg->sys_msec = job_stat[JobStatistics::sys_msec];
        msg->pfaults = job_stat[JobStatistics::sys_pfaults];
        end_status = job_stat[JobStatistics::exit_code];
    }

    close(client->pipeToChild());
    client->setPipeToChild(-1);
    string envforjob = client->job()->targetPlatform() + "/" + client->job()->environmentVersion();
    m_envsLastUse[envforjob] = time(NULL);

    bool r = send_scheduler(*msg);
    handle_end(client, end_status);
    delete msg;
    return r;
}

bool Daemon::handle_compile_file(Client *client, Msg *msg)
{
    CompileJob *job = dynamic_cast<CompileFileMsg *>(msg)->takeJob();
    assert(client);
    assert(job);
    client->setJob(job);

    if (client->status() == Client::CLIENTWORK) {
        assert(job->environmentVersion() == "__client");

        if (!send_scheduler(JobBeginMsg(job->jobID()))) {
            trace() << "can't reach scheduler to tell him about compile file job "
                    << job->jobID() << endl;
            return false;
        }

        // no scheduler is not an error case!
    } else {
        client->setStatus(Client::TOCOMPILE);
    }

    return true;
}

bool Daemon::handle_verify_env(Client *client, VerifyEnvMsg *msg)
{
    assert(msg);
    bool ok = verify_env(client->channel(), m_envBaseDir, msg->target, msg->environment, m_uid, m_gid);
    trace() << "Verify environment done, " << (ok ? "success" : "failure") << ", environment " << msg->environment
            << " (" << msg->target << ")" << endl;
    VerifyEnvResultMsg resultmsg(ok);

    if (!client->channel()->send_msg(resultmsg)) {
        log_error() << "sending verify end result failed.." << endl;
        return false;
    }

    return true;
}

bool Daemon::handle_blacklist_host_env(Client *client, Msg *msg)
{
    // just forward
    assert(dynamic_cast<BlacklistHostEnvMsg *>(msg));
    assert(client);

    if (!m_scheduler) {
        return false;
    }

    return send_scheduler(*msg);
}

void Daemon::handle_end(Client *client, int exitcode)
{
#ifdef ICECC_DEBUG
    trace() << "handle_end " << client->dump() << endl;
    trace() << dump_internals() << endl;
#endif
    m_fd2chan.erase(client->channel()->fd);

    if (client->status() == Client::TOINSTALL && client->pipeToChild() >= 0) {
        close(client->pipeToChild());
        client->setPipeToChild(-1);
        handle_transfer_env_done(client);
    }

    if (client->status() == Client::CLIENTWORK) {
        m_clients.setActiveProcesses(m_clients.activeProcesses() - 1);
    }

    if (client->status() == Client::WAITCOMPILE && exitcode == 119) {
        /* the client sent us a real good bye, so forget about the scheduler */
        client->setJobId(0);
    }

    /* Delete from the clients map before send_scheduler, which causes a
       double deletion. */
    if (!m_clients.erase(client->channel())) {
        log_error() << "client can't be erased: " << client->channel() << endl;
        flush_debug();
        log_error() << dump_internals() << endl;
        flush_debug();
        assert(false);
    }

    if (m_scheduler && client->status() != Client::WAITFORCHILD) {
        int job_id = client->jobId();

        if (client->status() == Client::TOCOMPILE) {
            job_id = client->job()->jobID();
        }

        if (client->status() == Client::WAITFORCS) {
            job_id = client->clientId(); // it's all we have
            exitcode = CLIENT_WAS_WAITING_FOR_CS; // this is the message
        }

        if (job_id > 0) {
            JobDoneMsg::from_type flag = JobDoneMsg::FROM_SUBMITTER;

            switch (client->status()) {
            case Client::TOCOMPILE:
                flag = JobDoneMsg::FROM_SERVER;
                break;
            case Client::UNKNOWN:
            case Client::GOTNATIVE:
            case Client::JOBDONE:
            case Client::WAITFORCHILD:
            case Client::LINKJOB:
            case Client::TOINSTALL:
                assert(false);   // should not have a job_id
                break;
            case Client::WAITCOMPILE:
            case Client::PENDING_USE_CS:
            case Client::CLIENTWORK:
            case Client::WAITFORCS:
                flag = JobDoneMsg::FROM_SUBMITTER;
                break;
            }

            trace() << "m_scheduler->send_msg( JobDoneMsg( " << client->dump() << ", " << exitcode << "))\n";

            if (!send_scheduler(JobDoneMsg(job_id, exitcode, flag))) {
                trace() << "failed to reach scheduler for remote job done msg!" << endl;
            }
        } else if (client->status() == Client::CLIENTWORK) {
            // Clientwork && !job_id == LINK
            trace() << "m_scheduler->send_msg( JobLocalDoneMsg( " << client->clientId() << ") );\n";

            if (!send_scheduler(JobLocalDoneMsg(client->clientId()))) {
                trace() << "failed to reach scheduler for local job done msg!" << endl;
            }
        }
    }

    delete client;
}

void Daemon::clear_children()
{
    while (!m_clients.empty()) {
        Client *cl = m_clients.first();
        handle_end(cl, 116);
    }

    while (m_currentKids > 0) {
        int status;
        pid_t child;

        while ((child = waitpid(-1, &status, 0)) < 0 && errno == EINTR) {}

        m_currentKids--;
    }

    // they should be all in clients too
    assert(m_fd2chan.empty());

    m_fd2chan.clear();
    m_newClientId = 0;
    trace() << "cleared children\n";
}

bool Daemon::handle_get_cs(Client *client, Msg *msg)
{
    GetCSMsg *umsg = dynamic_cast<GetCSMsg *>(msg);
    assert(client);
    client->setStatus(Client::WAITFORCS);
    umsg->client_id = client->clientId();
    trace() << "handle_get_cs " << umsg->client_id << endl;

    if (!m_scheduler) {
        /* now the thing is this: if there is no scheduler
           there is no point in trying to ask him. So we just
           redefine this as local job */
        client->setUseCSMsg(new UseCSMsg(umsg->target, "127.0.0.1", PORT,
                                         umsg->client_id, true, 1, 0));
        client->setStatus(Client::PENDING_USE_CS);
        client->setJobId(umsg->client_id);
        return true;
    }

    return send_scheduler(*umsg);
}

int Daemon::handle_cs_conf(ConfCSMsg *msg)
{
    m_maxSchedulerPong = msg->max_scheduler_pong;
    m_maxSchedulerPing = msg->max_scheduler_ping;
    m_benchSource = msg->bench_source;

    return 0;
}

bool Daemon::handle_local_job(Client *client, Msg *msg)
{
    client->setStatus(Client::LINKJOB);
    client->setOutFile(dynamic_cast<JobLocalBeginMsg *>(msg)->outfile);
    return true;
}

bool Daemon::handle_file_chunk_env(Client *client, Msg *msg)
{
    /* this sucks, we can block when we're writing
       the file chunk to the child, but we can't let the child
       handle MsgChannel itself due to MsgChannel's stupid
       caching layer inbetween, which causes us to loose partial
       data after the M_END msg of the env transfer.  */

    assert(client && client->status() == Client::TOINSTALL);

    if (msg->type == M_FILE_CHUNK && client->pipeToChild() >= 0) {
        FileChunkMsg *fcmsg = static_cast<FileChunkMsg *>(msg);
        ssize_t len = fcmsg->len;
        off_t off = 0;

        while (len) {
            ssize_t bytes = write(client->pipeToChild(), fcmsg->buffer + off, len);

            if (bytes < 0 && errno == EINTR) {
                continue;
            }

            if (bytes == -1) {
                log_perror("write to transfer env pipe failed. ");

                delete msg;
                msg = 0;
                handle_end(client, 137);
                return false;
            }

            len -= bytes;
            off += bytes;
        }

        return true;
    }

    if (msg->type == M_END) {
        close(client->pipeToChild());
        client->setPipeToChild(-1);
        return handle_transfer_env_done(client);
    }

    if (client->pipeToChild() >= 0) {
        handle_end(client, 138);
    }

    return false;
}

bool Daemon::handle_activity(Client *client)
{
    assert(client->status() != Client::TOCOMPILE);

    Msg *msg = client->channel()->get_msg();

    if (!msg) {
        handle_end(client, 118);
        return false;
    }

    bool ret = false;

    if (client->status() == Client::TOINSTALL && client->pipeToChild() >= 0) {
        ret = handle_file_chunk_env(client, msg);
    }

    if (ret) {
        delete msg;
        return ret;
    }

    switch (msg->type) {
    case M_GET_NATIVE_ENV:
        ret = handle_get_native_env(client, dynamic_cast<GetNativeEnvMsg *>(msg));
        break;
    case M_COMPILE_FILE:
        ret = handle_compile_file(client, msg);
        break;
    case M_TRANFER_ENV:
        ret = handle_transfer_env(client, msg);
        break;
    case M_GET_CS:
        ret = handle_get_cs(client, msg);
        break;
    case M_END:
        handle_end(client, 119);
        ret = false;
        break;
    case M_JOB_LOCAL_BEGIN:
        ret = handle_local_job(client, msg);
        break;
    case M_JOB_DONE:
        ret = handle_job_done(client, dynamic_cast<JobDoneMsg *>(msg));
        break;
    case M_VERIFY_ENV:
        ret = handle_verify_env(client, dynamic_cast<VerifyEnvMsg *>(msg));
        break;
    case M_BLACKLIST_HOST_ENV:
        ret = handle_blacklist_host_env(client, msg);
        break;
    default:
        log_error() << "not compile: " << (char)msg->type << "protocol error on client "
                    << client->dump() << endl;
        client->channel()->send_msg(EndMsg());
        handle_end(client, 120);
        ret = false;
    }

    delete msg;
    return ret;
}

int Daemon::answer_client_requests()
{
#ifdef ICECC_DEBUG

    if (m_clients.size() + m_currentKids) {
        log_info() << dump_internals() << endl;
    }

    log_info() << "clients " << m_clients.dump_per_status() << " " << m_currentKids
               << " (" << max_kids << ")" << endl;

#endif

    /* reap zombis */
    int status;

    while (waitpid(-1, &status, WNOHANG) < 0 && errno == EINTR) {}

    handle_old_request();

    /* collect the stats after the children exited m_icecreamLoad */
    if (m_scheduler) {
        maybe_stats();
    }

    fd_set listen_set;
    struct timeval tv;

    FD_ZERO(&listen_set);
    int max_fd = 0;

    if (m_tcpListenFd != -1) {
        FD_SET(m_tcpListenFd, &listen_set);
        max_fd = m_tcpListenFd;
    }

    FD_SET(m_unixListenFd, &listen_set);

    if (m_unixListenFd > max_fd) { // very likely
        max_fd = m_unixListenFd;
    }

    for (map<int, MsgChannel *>::const_iterator it = m_fd2chan.begin();
            it != m_fd2chan.end();) {
        int i = it->first;
        MsgChannel *c = it->second;
        ++it;
        /* don't select on a fd that we're currently not interested in.
           Avoids that we wake up on an event we're not handling anyway */
        Client *client = m_clients.find_by_channel(c);
        assert(client);
        int current_status = client->status();
        bool ignore_channel = current_status == Client::TOCOMPILE
                              || current_status == Client::WAITFORCHILD;

        if (!ignore_channel && (!c->has_msg() || handle_activity(client))) {
            if (i > max_fd) {
                max_fd = i;
            }

            FD_SET(i, &listen_set);
        }

        if (current_status == Client::WAITFORCHILD
                && client->pipeToChild() != -1) {
            if (client->pipeToChild() > max_fd) {
                max_fd = client->pipeToChild();
            }

            FD_SET(client->pipeToChild(), &listen_set);
        }
    }

    if (m_scheduler) {
        FD_SET(m_scheduler->fd, &listen_set);

        if (max_fd < m_scheduler->fd) {
            max_fd = m_scheduler->fd;
        }
    } else if (m_discover && m_discover->listen_fd() >= 0) {
        /* We don't explicitely check for m_discover->get_fd() being in
        the selected set below.  If it's set, we simply will return
        and our call will make sure we try to get the scheduler.  */
        FD_SET(m_discover->listen_fd(), &listen_set);

        if (max_fd < m_discover->listen_fd()) {
            max_fd = m_discover->listen_fd();
        }
    }

    tv.tv_sec = m_maxSchedulerPong;
    tv.tv_usec = 0;

    int ret = select(max_fd + 1, &listen_set, NULL, NULL, &tv);

    if (ret < 0 && errno != EINTR) {
        log_perror("select");
        return 5;
    }

    if (ret > 0) {
        bool had_scheduler = m_scheduler;

        if (m_scheduler && FD_ISSET(m_scheduler->fd, &listen_set)) {
            while (!m_scheduler->read_a_bit() || m_scheduler->has_msg()) {
                Msg *msg = m_scheduler->get_msg();

                if (!msg) {
                    log_error() << "scheduler closed connection\n";
                    close_scheduler();
                    clear_children();
                    return 1;
                }

                ret = 0;

                switch (msg->type) {
                case M_PING:

                    if (!IS_PROTOCOL_27(m_scheduler)) {
                        ret = !send_scheduler(PingMsg());
                    }

                    break;
                case M_USE_CS:
                    ret = scheduler_use_cs(static_cast<UseCSMsg *>(msg));
                    break;
                case M_GET_INTERNALS:
                    ret = scheduler_get_internals();
                    break;
                case M_CS_CONF:
                    ret = handle_cs_conf(static_cast<ConfCSMsg *>(msg));
                    break;
                default:
                    log_error() << "unknown scheduler type " << (char)msg->type << endl;
                    ret = 1;
                }

                delete msg;

                if (ret) {
                    return ret;
                }
            }
        }

        int listen_fd = -1;

        if (m_tcpListenFd != -1 && FD_ISSET(m_tcpListenFd, &listen_set)) {
            listen_fd = m_tcpListenFd;
        }

        if (FD_ISSET(m_unixListenFd, &listen_set)) {
            listen_fd = m_unixListenFd;
        }

        if (listen_fd != -1) {
            struct sockaddr cli_addr;
            socklen_t cli_len = sizeof cli_addr;
            int acc_fd = accept(listen_fd, &cli_addr, &cli_len);

            if (acc_fd < 0) {
                log_perror("accept error");
            }

            if (acc_fd == -1 && errno != EINTR) {
                log_perror("accept failed:");
                return EXIT_CONNECT_FAILED;
            }

            MsgChannel *c = Service::createChannel(acc_fd, &cli_addr, cli_len);

            if (!c) {
                return 0;
            }

            trace() << "accepted " << c->fd << " " << c->name << endl;

            Client *client = new Client;
            client->setClientId(++m_newClientId);
            client->setChannel(c);
            m_clients[c] = client;

            m_fd2chan[c->fd] = c;

            while (!c->read_a_bit() || c->has_msg()) {
                if (!handle_activity(client)) {
                    break;
                }

                if (client->status() == Client::TOCOMPILE
                        || client->status() == Client::WAITFORCHILD) {
                    break;
                }
            }
        } else {
            for (map<int, MsgChannel *>::const_iterator it = m_fd2chan.begin();
                    max_fd && it != m_fd2chan.end();)  {
                int i = it->first;
                MsgChannel *c = it->second;
                Client *client = m_clients.find_by_channel(c);
                assert(client);
                ++it;

                if (client->status() == Client::WAITFORCHILD
                        && client->pipeToChild() >= 0
                        && FD_ISSET(client->pipeToChild(), &listen_set)) {
                    max_fd--;

                    if (!handle_compile_done(client)) {
                        return 1;
                    }
                }

                if (FD_ISSET(i, &listen_set)) {
                    assert(client->status() != Client::TOCOMPILE);

                    while (!c->read_a_bit() || c->has_msg()) {
                        if (!handle_activity(client)) {
                            break;
                        }

                        if (client->status() == Client::TOCOMPILE
                                || client->status() == Client::WAITFORCHILD) {
                            break;
                        }
                    }

                    max_fd--;
                }
            }
        }

        if (had_scheduler && !m_scheduler) {
            clear_children();
            return 2;
        }

    }

    return 0;
}

bool Daemon::reconnect()
{
    if (m_scheduler) {
        return true;
    }

    if (!m_discover && m_nextSchedulerConnect > time(0)) {
        trace() << "timeout.." << endl;
        return false;
    }

#ifdef ICECC_DEBUG
    trace() << "reconn " << dump_internals() << endl;
#endif

    if (!m_discover || m_discover->timed_out()) {
        delete m_discover;
        m_discover = new DiscoverSched(m_netName, m_maxSchedulerPong, m_schedulerName);
    }

    m_scheduler = m_discover->try_get_scheduler();

    if (!m_scheduler) {
        log_warning() << "scheduler not yet found.\n";
        return false;
    }

    delete m_discover;
    m_discover = 0;
    sockaddr_in name;
    socklen_t len = sizeof(name);
    int error = getsockname(m_scheduler->fd, (struct sockaddr*)&name, &len);

    if (!error) {
        m_remoteName = inet_ntoa(name.sin_addr);
    } else {
        m_remoteName = string();
    }

    log_info() << "Connected to scheduler (I am known as " << m_remoteName << ")\n";
    m_currentLoad = -1000;
    gettimeofday(&last_stat, 0);
    m_icecreamLoad = 0;

    LoginMsg lmsg(PORT, determine_nodename(), m_machineName);
    lmsg.envs = available_environmnents(m_envBaseDir);
    lmsg.max_kids = max_kids;
    lmsg.noremote = m_noRemote;
    return send_scheduler(lmsg);
}

int Daemon::working_loop()
{
    for (;;) {
        reconnect();

        int ret = answer_client_requests();

        if (ret) {
            trace() << "answer_client_requests returned " << ret << endl;
            close_scheduler();
        }
    }

    // never really reached
    return 0;
}
