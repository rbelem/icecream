
#include "clients.h"


#include "../services/comm.h"
#include "../services/logging.h"

Clients::Clients()
    : m_activeProcesses(0)
{
}

Client *Clients::find_by_client_id(int id) const
{
    for (const_iterator it = begin(); it != end(); ++it) {
        if (it->second->clientId() == id) {
            return it->second;
        }
    }

    return 0;
}

Client *Clients::find_by_channel(MsgChannel *c) const
{
    const_iterator it = find(c);

    if (it == end()) {
        return 0;
    }

    return it->second;
}

Client *Clients::find_by_pid(pid_t pid) const
{
    for (const_iterator it = begin(); it != end(); ++it)
        if (it->second->childPid() == pid) {
            return it->second;
        }

    return 0;
}

Client *Clients::first()
{
    iterator it = begin();

    if (it == end()) {
        return 0;
    }

    Client *cl = it->second;
    return cl;
}

string Clients::dump_status(Client::Status s) const
{
    int count = 0;

    for (const_iterator it = begin(); it != end(); ++it) {
        if (it->second->status() == s) {
            count++;
        }
    }

    if (count) {
        return toString(count) + " " + Client::status_str(s) + ", ";
    }

    return string();
}

string Clients::dump_per_status() const
{
    string s;

    for (Client::Status i = Client::UNKNOWN; i <= Client::LASTSTATE;
            i = Client::Status(int(i) + 1)) {
        s += dump_status(i);
    }

    return s;
}

Client *Clients::get_earliest_client(Client::Status s) const
{
    // TODO: possibly speed this up in adding some sorted lists
    Client *client = 0;
    int min_client_id = 0;

    for (const_iterator it = begin(); it != end(); ++it) {
        if (it->second->status() == s && (!min_client_id || min_client_id > it->second->clientId())) {
            client = it->second;
            min_client_id = client->clientId();
        }
    }

    return client;
}

unsigned int Clients::activeProcesses() const
{
    return m_activeProcesses;
}

void Clients::setActiveProcesses(unsigned int num)
{
    m_activeProcesses = num;
}
