#!/usr/bin/python
# Re-implemented the demo JSV from SGE with my python parser.
# Orig copyright and license probably apply to this file
#
#  The Contents of this file are made available subject to the terms of
#  the Sun Industry Standards Source License Version 1.2
#
#  Sun Microsystems Inc., March, 2001
#
#
#  Sun Industry Standards Source License Version 1.2
#  =================================================
#  The contents of this file are subject to the Sun Industry Standards
#  Source License Version 1.2 (the "License"); You may not use this file
#  except in compliance with the License. You may obtain a copy of the
#  License at http://gridengine.sunsource.net/Gridengine_SISSL_license.html
#
#  Software provided under this License is provided on an "AS IS" basis,
#  WITHOUT WARRANTY OF ANY KIND, EITHER EXPRESSED OR IMPLIED, INCLUDING,
#  WITHOUT LIMITATION, WARRANTIES THAT THE SOFTWARE IS FREE OF DEFECTS,
#  MERCHANTABLE, FIT FOR A PARTICULAR PURPOSE, OR NON-INFRINGING.
#  See the License for the specific provisions governing your rights and
#  obligations concerning the Software.
#
#  The Initial Developer of the Original Code is: Sun Microsystems, Inc.
#
#  Copyright: 2008 by Sun Microsystems, Inc.
#
#  All Rights Reserved.

from JSV import JSV
class SimpleJSV(JSV):
    def jsv_on_start(self):
        pass
    def jsv_on_verify(self):
        do_correct = False
        do_wait = False
        if self.jsv_get_param('b') == 'y':
            self.jsv_reject("Binary job is rejected.")
            return
        if self.jsv_get_param('pe_name') != "":
            if int(self.jsv_get_param('pe_min')) % 16 != 0:
                self.jsv_reject('Parallel job does not request a multiple of 16 slots')
        if not self.jsv_is_param('l_hard'):
            context = self.jsv_get_param('CONTEXT')
            if self.jsv_sub_is_param('l_hard', 'h_vmem'):
                self.jsv_sub_del_param('l_hard', 'h_vmem')
                do_wait = True
                if context == "client":
                    self.jsv_log_info("h_vmem as hard resource requirement has been deleted")
            if self.jsv_sub_is_param('l_hard', 'h_data'):
                self.jsv_sub_del_param('l_hard', 'h_data')
                do_correct = True
                if context == "client":
                    self.jsv_log_info("h_data as hard resource requirement has been deleted")
        if self.jsv_is_param('ac'):
            context = self.jsv_get_param('CONTEXT')
            if self.jsv_sub_is_param('ac', 'a'):
                ac_a_value = int(self.jsv_sub_get_param('ac', 'a'))
                new_value = ac_a_value + 1
                self.jsv_sub_add_param('ac', 'a', new_value)
            else:
                self.jsv_sub_add_param('ac', 'a', 1)
            if self.jsv_sub_is_param('ac', 'b'):
                self.jsv_sub_del_param('ac', 'b')
            self.jsv_sub_add_param('ac', 'c')
        if do_wait:
            self.jsv_reject_wait("Job is rejected. It might be submitted later.")
        elif do_correct:
            self.jsv_correct("Jow was modified before it was accepted")
        else:
            self.jsv_accept("Job is accepted")
        return


j = SimpleJSV()
j.jsv_main()
