#ifndef CLIENT_H
#define CLIENT_H

#include <string>
#include <sys/types.h>
#ifdef __linux__
#  include <stdint.h>
#endif

class CompileJob;
class MsgChannel;
class UseCSMsg;

using namespace std;

struct Client {
public:
    /*
     * UNKNOWN: Client was just created - not supposed to be long term
     * GOTNATIVE: Client asked us for the native env - this is the first step
     * PENDING_USE_CS: We have a CS from scheduler and need to tell the client
     *          as soon as there is a spot available on the local machine
     * JOBDONE: This was compiled by a local client and we got a jobdone - awaiting END
     * LINKJOB: This is a local job (aka link job) by a local client we told the scheduler about
     *          and await the finish of it
     * TOINSTALL: We're receiving an environment transfer and wait for it to complete.
     * TOCOMPILE: We're supposed to compile it ourselves
     * WAITFORCS: Client asked for a CS and we asked the scheduler - waiting for its answer
     * WAITCOMPILE: Client got a CS and will ask him now (it's not me)
     * CLIENTWORK: Client is busy working and we reserve the spot (job_id is set if it's a scheduler job)
     * WAITFORCHILD: Client is waiting for the compile job to finish.
     */
    enum Status {
        UNKNOWN,
        GOTNATIVE,
        PENDING_USE_CS,
        JOBDONE,
        LINKJOB,
        TOINSTALL,
        TOCOMPILE,
        WAITFORCS,
        WAITCOMPILE,
        CLIENTWORK,
        WAITFORCHILD,
        LASTSTATE = WAITFORCHILD
    };

    Client();
    ~Client();

    static string status_str(Status status);
    string dump() const;

    uint32_t jobId() const;
    void setJobId(uint32_t id);

    string outFile() const;
    void setOutFile(const string &outFile);

    MsgChannel *channel() const;
    void setChannel(MsgChannel *channel);

    CompileJob *job() const;
    void setJob(CompileJob *job);

    UseCSMsg *useCSMsg() const;
    void setUseCSMsg(UseCSMsg *useCSMsg);

    int clientId() const;
    void setClientId(int id);

    Status status() const;
    void setStatus(Status status);

    int pipeToChild() const;
    void setPipeToChild(int value);

    pid_t childPid() const;
    void setChildPid(pid_t pid);

private:
    uint32_t m_jobId;
    string m_outFile; // only useful for LINKJOB or TOINSTALL
    MsgChannel *m_channel;
    CompileJob *m_job;
    UseCSMsg *m_useCSMsg;
    int m_clientId;
    Status m_status;
    int m_pipeToChild; // pipe to child process, only valid if WAITFORCHILD or TOINSTALL
    pid_t m_childPid;
};

#endif
