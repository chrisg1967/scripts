#!/usr/bin/python
import json
import requests
import time
import hashlib
import getpass
import sys
import winrm
import threading
import re
import argparse

#
# winrm library requirement: install pywinrm with the followinng command:
# pip install pywinrm
#

class site_api(object):

    def data(self, url_type):
        #
        # site_api.data will retrieve information from site manager api
        # and return either the instance json, or the site json info
        # depending on the information needed
        #
        if url_type == 'site':
            url = base_url + '/v1/manage/site/read' + apicreds
        elif url_type == 'inst':
            url = base_url + '/v1/manage/instance/read' + apicreds
        
        try:
            get = requests.get(url=url).json()
            return get
        except:
            raise


    def instances(self, data, site):
        #
        # site_api.instances iterates through the site api json
        # to get a list of instances at the given site
        #
        inst_list = []
        for n in range(len(data)):
            if data[n]['site']['siteid'] == site:
                inst_list.append(data[n]['instanceid'])

        inst_list.sort()
        return inst_list


    def attributes(self, instanceid, data):
        #
        # site_api.attributes gets instance hash and returns all provided attributes
        #
        for n in range(len(data)):
            if data[n]['instanceid'] == instanceid:
                return data[n]


    def siteid(self, site, data):
        #
        # site_api.siteid checks if the siteid provided is the new 7 digit style or
        # the old dynatrace siteid style. if it is the old dt style, it will check
        # the json data for the dynatrace siteid, and return the new style. if it is
        # the new style, it returns that instead
        #
        try:
            for n in range(len(data)):
                if len(str(site)) == 7:
                    if data[n]['siteid'] == site:
                        siteid = site
                else:
                    if int((data[n]['siteiddt'])) == site:
                        siteid = data[n]['siteid']
            return siteid
        except UnboundLocalError:
            raise


class status(object):

    def __init__(self, instanceid, siteid):
        self.instanceid = instanceid
        self.siteid     = siteid


    def get(self):
        #
        # status.get checks instance hash to return the current agent status
        #
        api    = site_api()
        data   = api.data(url_type='inst')
        latest = api.attributes(instanceid=self.instanceid, data=data)
        
        return latest['statuscode']['id']


    def set(self, statuscode):
        #
        # status.change will change the status of the agent
        # setting it to active or inactive based on the request
        #
        post_url = ('%(base)s/v1/manage/instance/update%(login)s' % {
            'base': base_url,
            'login': apicreds
        })

        if statuscode == 1:
            inststatus = 'Active'
            statusname = 'ACTIVE'
        elif statuscode == 10:
            inststatus = 'Inactive'
            statusname = 'MAINT-CA'

        payload = {
            "instanceid": self.instanceid,
            "inststatus": inststatus,
            "statuscode": {
                "id": statuscode,
                "name": statusname
            },
            "site": { 
                "id": self.siteid
            }
        }

        p = json.dumps(payload)
        r = requests.post(post_url, data = p)
        r.raise_for_status()

        try:
            self.balance()
        except:
            pass


    def balance(self):
        if cycle == 'day':
            balancer_host = ''
        if cycle == 'sprint':
            balancer_host = ''
        if cycle == 'eap':
            balancer_host = ''
        if cycle == 'prod':
            balancer_host = ''


        balance_url = 'http://%(host)s:8082/balancer/?rebalance=%(site)d' % {
            'host': balancer_host,
            'site': self.siteid
        }

        try:
            requests.get(url=balance_url)
        except:
            raise



class splunk(object):

    def __init__(self, hostname):
        self.hostname      = hostname
        self.lookback      = '-90s'
        self.splunk_user   = ''
        self.splunk_pass   = ''
        self.splunk_server = ''
        self.splunk_url    = self.splunk_server + "/servicesNS/nobody/dyndiag/search/jobs/export"

    def check_id(self):
        splunk_search = 'search `GetCAInstanceid(environment=%(cycle)s,lookback=%(lookback)s,hostname=%(hostname)s)`' % {
            'cycle': cycle,
            'lookback': self.lookback,
            'hostname': self.hostname }

        payload = {
            'search': splunk_search,
            'preview': '0',
            'output_mode': 'json'
        }

        splunk_data = requests.post(self.splunk_url, auth=(self.splunk_user,self.splunk_pass),data=payload)
        splunk_arr = splunk_data.text.strip().split('\n')

        for line in splunk_arr:
            splunk_json = json.loads(line)
            
            try:
                dict = splunk_json['result']
                id   = int(dict['instanceid'])
                return id
            except:
                print('No key in Splunk for instance id of host: %s' % self.hostname)
                raise
        

    def transactions(self, status):
        #
        # splunk.transactions will query splunk api to check if the agent
        # has transactions occuring, to determine if it is either actually
        # active or inactive for verification purposes
        #
        splunk_search = 'search `GetCAMeasurements(environment=%(cycle)s,lookback=%(lookback)s,hostname=%(hostname)s)`' % {
            'cycle': cycle,
            'lookback': self.lookback,
            'hostname': self.hostname }

        payload = {
            'search': splunk_search,
            'preview': '0',
            'output_mode': 'json'
        }

        splunk_data = requests.post(self.splunk_url, auth=(self.splunk_user,self.splunk_pass),data=payload)
        splunk_arr = splunk_data.text.strip().split('\n')

        for line in splunk_arr:
            splunk_json = json.loads(line)
            
            try:
                dict   = splunk_json['result']
                mstart = int(dict['measurementsstarted'])
                mfin   = int(dict['measurementsfinished'])

                sys.stdout.write('\rTransactions: %(start)d,%(fin)d' % { 'start': mstart, 'fin': mfin })
                sys.stdout.flush()

                if status == 1:
                    if mstart != 0 and mfin != 0:
                        return 0
                    else:
                        return 1
                else:
                    if mstart == 0 and mfin == 0:
                        return 0
                    else:
                        return 1
            except KeyError:
                raise
            except KeyboardInterrupt:
                raise


    def check(self, status):
        #
        # splunk.check checks for transactions over time
        #
        print("Checking for transactions on %s" % self.hostname)
        
        for wait in range(20):
            try:
                time.sleep(30)
                if self.transactions(status) == 0:
                    return 0
                    break
            except KeyboardInterrupt:
                break
            except:
                continue
        else:
            return 1



class connect(object):

    def __init__(self, hostname, user, password):
        self.hostname = hostname
        self.user     = user
        self.password = password


    def session(self):
        #
        # connect.session creates a winrm connection to a windows host
        #

        #
        # add check for blank user/passwd and prompt if blank
        #
        try:
            cmd = winrm.Session(self.hostname, auth=(self.user, self.password), transport='ntlm')
            return cmd
        except winrm.exceptions.InvalidCredentialsError:
            raise
        except requests.exceptions.ConnectionError:
            raise

    def puppet(self, session):
        #
        # connect.puppet will run puppet via powershell using the winrm session provided
        #
        lock_err = 'The process cannot access the file because it is being used by another process.'

        if noop:
            pupcmd = 'puppet agent -t --noop'
        elif not noop:
            pupcmd = 'puppet agent -t --no-noop'

        run = session.run_ps(pupcmd)

        while run.std_err == lock_err:
            print(run.std_err)
            print('Puppet lock error on %s, retrying in 30 seconds' % self.hostname)
            x += 1
            time.sleep(30)
            puppet(session)
            if run.std_err != lock_err:
                break
            if x == 4:
                print('Puppet locked. Bailing.')
                raise OSError
                break
        else:
            print(run.std_out)

    def ps_reboot(self, session):
        cmd = session.run_ps('Restart-Computer -Force')
        if cmd.status_code != 0:
            raise SystemError

    def check(self, session):
        cmd = session.run_ps('Test-Connection 127.0.0.1 -Count 1')
        if cmd.status_code != 0:
            raise SystemError


def reboot(hostname, user, password):
    x     = 0
    y     = 0
    loops = 61

    try:
        c = connect(hostname, user, password)
        c.ps_reboot(c.session())
    except SystemError:
        raise

    print('Rebooting %s' % hostname)

    while y < loops:

        try:
            ct = connect(hostname, user, password)
            ct.check(ct.session())
            y += 1
            sys.stdout.write('\rWaiting: [%(prog)d/%(loops)d] %(count)s' % { 'prog': y, 'loops': loops, 'count': ('.' * y) })
            sys.stdout.flush()
            time.sleep(10)
            continue
        except:
            print('server shutdown')
            shutdown = 0
            break
    else:
        print('%s has not successfully shutdown' % hostname)
        shutdown = 1
        raise OSError

    if shutdown == 0:
        print('waiting for %s to start' % hostname)
        time.sleep(180)

        while x < loops:
            try:
                ct = connect(hostname, user, password)
                ct.check(ct.session())
                print('%s has booted up' % hostname)
                break
            except:
                x += 1
                sys.stdout.write('\rwaiting for restart - %d' % x)
                sys.stdout.write('\rWaiting: [%(prog)d/%(loops)d] %(count)s' % { 'prog': x, 'loops': loops, 'count': ('.' * y) })
                sys.stdout.flush()
                time.sleep(10)
                continue
        else:
            print('%s has not successfully restarted' % hostname)
            raise OSError


def update(hostname, instanceid, site, single_instance, user, password, action):
    #
    # update runs the commands to complete the upgrade per host
    #
    sp = splunk(hostname)
    st = status(instanceid, siteid=site)
    
    print('Updating agent:')
    print('Hostname: %s' % hostname)
    print('Instance: %s' % instanceid)

    if st.get() == 1:
        reactivate = True
        print("Inactivating %s" % hostname)
        st.set(statuscode=inactive)
        st.balance()
        if not single_instance:
            sp.check(status=inactive)
    else:
        reactivate = False

    try:
        ct = connect(hostname, user, password)
    except winrm.exceptions.InvalidCredentialsError:
        print('Invaild credentials, exiting')
    except:
        print('WinRM session connection failure, exiting')
        sys.exit()

    if action == 'reboot':
        try:
            reboot(hostname, user, password, ct)
        except OSError:
            raise
    elif action == 'update':
        print('Starting puppet update on %s' % hostname)

        try:
            ct.puppet(ct.session())
        except OSError:
            print('puppet run failed')
            raise

    if reactivate:
        print("Activating %s" % hostname)
        st.set(statuscode=active)
        st.balance()
        time.sleep(120)
        sp.check(status=active)

    print('Upgrade complete on %s' % hostname)



def main(sites, action, user, password):
    #
    # main calls for a list of instances at a site, then calls
    # upgrade to upgrade each instance one at a time
    #
    for user_site in map(int, sites.split(',')):
        try:
            api       = site_api()
            site_data = api.data(url_type='site')
            inst_data = api.data(url_type='inst')
            site      = api.siteid(site=user_site, data=site_data)
            instances = api.instances(data=inst_data, site=site)
        except:
            print('Error collecting data from site manager')
            continue

        if len(instances) == 1:
            single = True
        else:
            single = False

        for i in range(len(instances)):
            attr       = api.attributes(instances[i], data=inst_data)
            instanceid = attr['instanceid']
            hostname   = (attr['hostname']).lower()
            stat       = status(instanceid, siteid=site)
            work       = True
            pattern    = re.compile('^.*\.(prod|dev)\.saasapm\.com$')
            sp         = splunk(hostname)

            if not pattern.match(hostname):
                print('Invalid hostname: %s' % hostname)
                continue

            try:
                if sp.check_id() != instanceid:
                    print('Splunk instance id and site manager id do not match for %s. Moving on to next' % hostname)
                    continue
            except:
                print('Splunk check error on %s, moving on' % hostname)
                print('splunkid: %d' % sp.check_id())
                print('instanceid: %d' % instanceid)
                continue

            try:
                update(instanceid=instanceid,
                       hostname=hostname,
                       site=site,
                       single_instance=single,
                       user=user,
                       password=password,
                       action=action)
            except (KeyError, OSError):
                print('Update failures on %s' % hostname)
                continue

        print('Site %d completed' % user_site)


def setup(env, sites, action, user, password):
    global stop
    global kill
    global active
    global inactive
    global apicreds
    global base_url
    global cycle
    global noop

    cycle    = env
    inactive = int(10)
    active   = int(1)
    kill     = False
    stop     = False
    noop     = False

    if cycle in { '', '', '' }:
        dt_domain  = ''
    elif cycle in { '', ''. '' }:
        dt_domain  = ''

    apicreds = ''
    base_url = 'https://' + cycle + dt_domain

    main(sites, action, user, password)


parser = argparse.ArgumentParser()

# group code is a placeholder for allowing single instance update/reboot
#group = parser.add_mutually_exclusive_group(required=True)
#group.add_argument('--sites', action="store", type=str, default=None)
#group.add_argument('--instanceid', action="store", type=int, default=None)

parser.add_argument('--sites', action="store", type=str, required=True)
parser.add_argument('--user', action="store", type=str, required=True)
parser.add_argument('--env', action="store", type=str, required=True, choices=['day', 'sprint', 'eap', 'prod'])
parser.add_argument('--action', action="store", type=str, required=True, choices=['update', 'reboot'])

args     = parser.parse_args()
user     = ('%s@saasapm' % args.user)
password = getpass.getpass('Password: ')

setup(env=args.env, sites=args.sites, action=args.action, user=user, password=password)


