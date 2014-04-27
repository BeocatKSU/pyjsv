#!/usr/bin/python

import re, sys, tempfile

class JSV( object ):
    "Control the Job Submission Verification steps"
    _jsv_cli_params = ( "a", "ar", "A", "b", "ckpt", "cwd", "C", "display",
                        "dl", "e", "hard", "h", "hold_jid", "hold_jid_ad", "i",
                        "inherit", "j", "js", "m", "M", "masterq", "notify",
                        "now", "N", "noshell", "nostdin", "o", "ot", "P", "p",
                        "pty", "R", "r", "shell", "sync", "S", "t", "tc",
                        "terse", "u", "w", "wd" )
    _jsv_mod_params = ( "ac", "l_hard", "l_soft", "q_hard", "q_soft", "pe_min",
                        "pe_max", "pe_name", "binding_strategy", "binding_type",
                        "binding_amount", "binding_socket", "binding_core",
                        "binding_step", "binding_exp_n" )
    _jsv_add_params = ( "CLIENT", "CONTEXT", "GROUP", "VERSION", "JOB_ID",
                        "SCRIPT", "CMDARGS", "USER" )
    def __init__(self, logging=False) :
        self.Log = False
        if logging is True :
            self.Log = True
            self.tempfile = tempfile.NamedTemporaryFile(delete=False)
        self.state = "initialized"
        self.env = {}
        self.param = {}
    def jsv_script_log(self, error) :
        if self.Log:
            self.tempfile.write(error)
    def jsv_main( self ) :
        self.jsv_script_log(" started on  date")
        self.jsv_script_log("")
        self.jsv_script_log("This file contains logging output from a GE JSV script. Lines beginning")
        self.jsv_script_log("with >>> contain the data which was sent by a command line client or")
        self.jsv_script_log("sge_qmaster to the JSV script. Lines beginning with <<< contain data")
        self.jsv_script_log("which is sent for this JSV script to the client or sge_qmaster")
        self.jsv_script_log("")
        _jsv_quit = False
        try :
            while (not _jsv_quit) :
                iput = sys.stdin.readline().strip('\n')
                if len(iput) == 0:
                    continue
                self.jsv_script_log(">>> {}".format(iput))
                jsv_line = re.split('\s*', iput, maxsplit=3)
                if jsv_line[0] == "QUIT" :
                    _jsv_quit = True
                elif jsv_line[0] == "PARAM" :
                    self.jsv_handle_param_command(jsv_line[1], jsv_line[2])
                elif jsv_line[0] == "ENV" :
                    jsv_data = re.split('\s*', jsv_line[2], maxsplits=2)
                    self.jsv_handle_env_command(jsv_line[1], jsv_data[0], jsv_data[1])
                elif jsv_line[0] == "START" :
                    self.jsv_handle_start_command()
                elif jsv_line[0] == "BEGIN" :
                    self.jsv_handle_begin_command()
                elif jsv_line[0] == "SHOW" :
                    self.jsv_show_params()
                    self.jsv_show_envs()
                else:
                    self.jsv_send_command("ERROR JSV script got unknown command " + jsv_line[0])
        except (EOFError) :
            pass
        self.jsv_script_log("$0 is terminating on `date`")
        if self.Log:
            self.tempfile.close()
    def jsv_send_command(self, command):
        print(command)
        sys.stdout.flush()
        self.jsv_script_log("<<< {}".format(command))
    def jsv_handle_start_command(self):
        if self.state == "initialized":
            self.jsv_on_start()
            self.jsv_send_command("STARTED")
            self.state = "started"
        else:
            self.jsv_send_command("ERROR JSV script got START but is in state {}".format(self.state))
    def jsv_handle_begin_command(self):
        if self.state == "started":
            self.state = "verifying"
            self.jsv_on_verify()
            self.jsv_clear_params()
            self.jsv_clear_envs()
        else:
            self.jsv_send_command("ERROR JSV script got BEGIN but is in state {}".format(self.state))
    def jsv_handle_param_command(self, param, value) :
        self.param[param] = {}
        if ',' in value:
            values = value.split(',')
            for v in values:
                if '=' in v:
                    t = v.split('=')
                    self.param[param][t[0]] = t[1]
                else:
                    self.param[param][v] = None
        else:
            if '=' in value:
                t = value.split('=')
                self.param[param][t[0]] = t[1]
            else:
                self.param[param][value] = None
    def jsv_handle_env_command(self, action, name, data) :
        if action == 'DEL':
            if name in self.env.keys():
                del(self.env[name])
        else:
            self.env[name] = data
    def jsv_on_verify(self):
        self.jsv_accept()
        pass
    def jsv_on_start(self):
        pass
    def jsv_accept(self, reason):
        if self.state == "verifying":
            self.jsv_send_command("RESULT STATE ACCEPT {}".format(reason))
            self.state = "initialized"
        else:
            self.jsv_send_command("ERROR JSV script tried to send RESULT but was in state {}".format(self.state))
    def jsv_correct(self, reason):
        if self.state == "verifying":
            self.jsv_send_command("RESULT STATE CORRECT {}".format(reason))
            self.state = "initialized"
        else:
            self.jsv_send_command("ERROR JSV script tried to send RESULT but was in state {}".format(self.state))
    def jsv_reject(self, reason):
        if self.state == "verifying":
            self.jsv_send_command("RESULT STATE REJECT {}".format(reason))
            self.state = "initialized"
        else:
            self.jsv_send_command("ERROR JSV script tried to send RESULT but was in state {}".format(self.state))
    def jsv_reject_wait(self, reason):
        if self.state == "verifying":
            self.jsv_send_command("RESULT STATE REJECT_WAIT {}".format(reason))
            self.state = "initialized"
        else:
            self.jsv_send_command("ERROR JSV script tried to send RESULT but was in state {}".format(self.state))
    def jsv_clear_envs(self):
        self.env = {}
    def jsv_clear_params(self):
        self.param = {}
    def jsv_is_env(self, var):
        return var in self.env.keys()
    def jsv_get_env(self, var):
        if self.jsv_is_env(var):
            return self.env['var']
        return None
    def jsv_add_env(self, var, val):
        if not self.jsv_is_env(var):
            self.env[var] = val
            self.jsv_send_command('ENV ADD {} {}'.format(var, val))
    def jsv_mod_env(self, var, val):
        if self.jsv_is_env(var):
            self.env[var] = val
            self.jsv_send_command('ENV MOD {} {}'.format(var, val))
    def jsv_del_env(self, var):
        if self.jsv_is_env(var):
            del(self.env[var])
            self.jsv_send_command('ENV DEL {}'.format(var))
    def jsv_show_params(self):
        for k, v in self.param:
            self.jsv_log_info("got param {}={}".format(k,v))
    def jsv_show_envs(self):
        for k, v in self.env:
            self.jsv_log_info("got env {}={}".format(k,v))
    def jsv_log_info(self, message):
        self.jsv_send_command("LOG INFO {}".format(message))
    def jsv_log_warning(self, message):
        self.jsv_send_command("LOG WARNING {}".format(message))
    def jsv_log_error(self, message):
        self.jsv_send_command("LOG ERROR {}".format(message))
    def jsv_send_env(self):
        self.jsv_send_command("SEND ENV")
    def jsv_is_param(self, param):
        return param in self.param.keys()
    def jsv_get_param(self, param):
        if self.jsv_is_param(param):
            if len(list(self.param[param].keys())) == 1 and self.param[param][list(self.param[param].keys())[0]] is None:
                return list(self.param[param].keys())[0]
            return self.param[param]
        return None
    def jsv_set_param(self, param, val):
        self.param[param] = val
        self.jsv_send_command("PARAM {} {}".format(param, val))
    def jsv_del_param(self, param):
        if self.jsv_is_param(param):
            del(self.param[param])
            self.jsv_send_command("PARAM {}".format(param))
    def jsv_sub_is_param(self, param, var):
        if self.jsv_is_param(param):
            v = self.jsv_get_param(param)
            if type(v) is dict:
                return var in v.keys()
        return False
    def jsv_sub_get_param(self, param, var):
        if self.jsv_sub_is_param(param, var):
            return self.jsv_get_param(param)[var]
        return None
    def jsv_sub_add_param(self, param, var, val):
        if self.jsv_is_param(param):
            self.param[param][var] = val
        else:
            self.param[param] = {var: val}
        args = []
        for item in self.param[param].items():
            if item[1] is None:
                args.append(item[0])
            else:
                args.append('='.join(item))
        args = ','.join(args)
        self.jsv_send_command("PARAM {} {}".format(param, args))
    def jsv_sub_del_param(self, param, var):
        if self.jsv_is_param(param) and self.jsv_sub_is_param(param, var):
            del(self.param[param][var])
        args = []
        for item in self.param[param].items():
            if item[1] is None:
                args.append(item[0])
            else:
                args.append('='.join(item))
        args = ','.join(args)
        self.jsv_send_command("PARAM {} {}".format(param, args))
