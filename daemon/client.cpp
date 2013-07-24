
#include "client.h"

#include <assert.h>
#include <unistd.h>

#include "../services/comm.h"
#include "../services/job.h"
#include "../services/logging.h"

Client::Client()
    : m_jobId(0)
    , m_outFile()
    , m_channel(0)
    , m_job(0)
    , m_useCSMsg(0)
    , m_clientId(0)
    , m_status(UNKNOWN)
    , m_pipeToChild(-1)
    , m_childPid(-1)
{
}

static string status_str(Client::Status status) {
    switch (status) {
    case Client::UNKNOWN:
        return "unknown";
    case Client::GOTNATIVE:
        return "gotnative";
    case Client::PENDING_USE_CS:
        return "pending_use_cs";
    case Client::JOBDONE:
        return "jobdone";
    case Client::LINKJOB:
        return "linkjob";
    case Client::TOINSTALL:
        return "toinstall";
    case Client::TOCOMPILE:
        return "tocompile";
    case Client::WAITFORCS:
        return "waitforcs";
    case Client::CLIENTWORK:
        return "clientwork";
    case Client::WAITCOMPILE:
        return "waitcompile";
    case Client::WAITFORCHILD:
        return "waitforchild";
    }

    assert(false);
    return string(); // shutup gcc
}

Client::~Client()
{
    m_status = (Status) - 1;
    delete m_channel;
    m_channel = 0;
    delete m_useCSMsg;
    m_useCSMsg = 0;
    delete m_job;
    m_job = 0;

    if (m_pipeToChild >= 0) {
        close(m_pipeToChild);
    }
}

string Client::dump() const
{
    string ret = status_str(m_status) + " " + m_channel->dump();

    switch (m_status) {
    case LINKJOB:
        return ret + " CID: " + toString(m_clientId) + " " + m_outFile;
    case TOINSTALL:
        return ret + " " + toString(m_clientId) + " " + m_outFile;
    case WAITFORCHILD:
        return ret + " CID: " + toString(m_clientId) + " PID: " + toString(m_childPid) + " PFD: " + toString(m_pipeToChild);
    default:

        if (m_jobId) {
            string jobs;

            if (m_useCSMsg) {
                jobs = " CS: " + m_useCSMsg->hostname;
            }

            return ret + " CID: " + toString(m_clientId) + " ID: " + toString(m_jobId) + jobs;
        } else {
            return ret + " CID: " + toString(m_clientId);
        }
    }

    return ret;
}

uint32_t Client::jobId() const
{
    return m_jobId;
}

void Client::setJobId(uint32_t id)
{
    m_jobId = id;
}

string Client::outFile() const
{
    return m_outFile;
}

void Client::setOutFile(const string &outFile)
{
    m_outFile = outFile;
}

MsgChannel *Client::channel() const
{
    return m_channel;
}

void Client::setChannel(MsgChannel *channel)
{
    m_channel = channel;
}

CompileJob *Client::job() const
{
    return m_job;
}

void Client::setJob(CompileJob *job)
{
    m_job = job;
}

UseCSMsg *Client::useCSMsg() const
{
    return m_useCSMsg;
}

void Client::setUseCSMsg(UseCSMsg *useCSMsg)
{
    m_useCSMsg = useCSMsg;
}

int Client::clientId() const
{
    return m_clientId;
}

void Client::setClientId(int id)
{
    m_clientId = id;
}

Client::Status Client::status() const
{
    return m_status;
}

void Client::setStatus(Client::Status status)
{
    m_status = status;
}

int Client::pipeToChild() const
{
    return m_pipeToChild;
}

void Client::setPipeToChild(int value)
{
    m_pipeToChild = value;
}

pid_t Client::childPid() const
{
    return m_childPid;
}

void Client::setChildPid(pid_t pid)
{
    m_childPid = pid;
}
