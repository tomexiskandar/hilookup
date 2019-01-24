"""
a module that finds matching based upon:
 - user based assumption on string restructuring
 - user based score weighting
 - character cleansing
 - the input and output as dataframe
"""

import pandas as pd
import re
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
from multiprocessing import Pool, freeze_support,Value
import multiprocessing
import time
from datetime import datetime
import os

class Row_Column:
    def __init__(self,value\
                     ,chars_tostrip=None\
                     ,wordindex_group_dict=None\
                     ,wordindex_simple=None\
                     ,baseword_list=None\
                     ,replace_dict=None\
                     ,valid_value_list=None):
        self.word_group_list = []
        self.word_col_list = []
        self.word_col_lod = []
        self.baseword = None
        self.split_words(value\
                         ,chars_tostrip=chars_tostrip\
                         ,wordindex_group_dict=wordindex_group_dict\
                         ,wordindex_simple=wordindex_simple\
                         ,baseword_list=baseword_list\
                         ,replace_dict=replace_dict)


    def split_words(self,value\
                        ,chars_tostrip=None\
                        ,wordindex_group_dict=None\
                        ,wordindex_simple=None\
                        ,baseword_list=None\
                        ,replace_dict=None):
        r"""create a dictionary that holds splitted words
        this list will be a split of seperator"""

        # replace any untested chars
        if chars_tostrip is not None:
            for ch in chars_tostrip:
                value = value.replace(ch,"").strip()

        if baseword_list is not None:
            baseword = self.get_filtered_baseword(value,baseword_list)
            if baseword is not None:
                #print("opps... baseword is not available for {}".format(value))
                self.baseword = baseword
                # future enhancement:
                # this could be used to set validity of matching.
                # and a param (boolean) can be passed to determine
                # whether the user wants to use his basewords or not.
            else:
                #print("good... baseword is available for {}".format(value))
                self.baseword = None

        # truncate word(s) if required (yes! using replace function)
        if replace_dict is not None:
            value = self.replace_word(value,replace_dict)

        has_complex_word = False
        if wordindex_group_dict is not None:
            for sep in wordindex_group_dict:
                if value.find(sep) >= 0:
                    has_complex_word = True
                    self.split_group_words(value,sep,wordindex_group_dict)

        # if there is no complex words
        if len(self.word_group_list) == 0:
            self.word_group_list.append(value.strip())
        else:
            self.add_whole_word(value.strip())
        # print("wglist_outside",self.word_group_list)
        # # quit()
        # for i in self.word_group_list:
        #     print(i)
        # print("------")
        # quit()
        for i,wg_item in enumerate(self.word_group_list):
            # create index for simple words
            si = i
            # add the group word
            self.word_col_list.append(wg_item)
            _dict = {}
            if has_complex_word:
                si = i + 1
                _dict['gi'] = si
            else:
                _dict["gi"] = 0

            _dict["wi"] = 0
            _dict["val"] = wg_item
            _dict["bw"] = self.baseword
            self.word_col_lod.append(_dict)
            # add the splitted words
            if len(wg_item.split()) > 1:
                self.split_simple_words(si,wg_item,wordindex_simple)
        # for i in self.word_col_lod:
        #     print(i)
        # quit()

    def split_group_words(self,value,separator\
                            ,wordindex_group_dict=None):
        # print("len",len(self.word_group_list))
        # print("sep:",separator)
        # print("value",value)
        if len(self.word_group_list) == 0:
            if wordindex_group_dict[separator] == 'right-to-left':
                _value_list = reversed(value.split(separator))
            else:
                _value_list = value.split(separator)
            # strip any None member
            _value_list = list(filter(None,_value_list))


            # print("hello")
            for item in _value_list:
                #_list.append(item)
                if item not in self.word_group_list:
                    self.word_group_list.append(item.strip())
            # print("wglist_inside 0",self.word_group_list)
            return

        if len(self.word_group_list) > 0: #type(value) is list:
            _list_temp =  []
            for wg in self.word_group_list:
                #idx = self.word_group_list.index(wg)
                if wg.find(separator) >= 0: #if there is a separator found
                    if wordindex_group_dict[separator]=='right-to-left':
                        wg_list = list(reversed(wg.split(separator)))
                    else:
                        wg_list = wg.split(separator)
                    # strip any none member
                    wg_list = list(filter(None,wg_list))
                    for x in wg_list:
                        if x not in self.word_group_list:
                            _list_temp.append(x.strip())
                else: # copy back the wg
                    _list_temp.append(wg)
            # print("wglist_inside >0",self.word_group_list)
            # print("list temp",_list_temp)
            self.word_group_list = _list_temp




    def split_simple_words(self,gi,value,wordindex_simple):
        if wordindex_simple is None or wordindex_simple=='right-to-left':
            value = list(reversed(value.split()))
        else:
            value = value.split()
        #print("after",value)
        for item in value:
            # print("item",item)

            #if item not in self.word_col_list:
            self.word_col_list.append(item)
            _dict = {}
            _dict["gi"] = gi
            _dict["wi"] = value.index(item)+1
            _dict["val"] = item.strip()
            _dict["bw"] = self.baseword
            #self.word_col_list.append(item)
            self.word_col_lod.append(_dict)
            # for i in self.word_col_lod:
            #     print("--->", i)

    def add_whole_word(self,value):
        _dict = {}
        _dict["gi"] = 0
        _dict["wi"] = 0
        _dict["val"] = value
        _dict["bw"] = self.baseword
        self.word_col_lod.append(_dict)

    def get_filtered_baseword(self,value,baseword_list=None):
        for i,bw in enumerate(baseword_list):
            if value[:len(bw)] == bw:
                return bw


    def replace_word(self,value,replace_dict=None):
        for k,v in replace_dict.items():
            if k.upper() in value.upper():
                value = value.replace(k,v)
        return value


class PandasSeries:
    def __init__(self,rowid,row_data):
        self.rowid = rowid
        self.row_data  = row_data
class ProcessUnit:
    def __init__(self,rowid,row_data,src_list):
        self.rowid = rowid
        self.row_data  = row_data
        self.src_list = src_list

class Row():
    def __init__(self,rowid,row_data\
                     ,chars_tostrip=None\
                     ,fieldname_toevaluate_list=None\
                     ,wordindex_group_dict=None\
                     ,wordindex_simple=None\
                     ,baseword_list=None\
                     ,replace_dict=None):
        # print(baseword_list)
        # quit()
        self.fieldname_toevaluate_list = fieldname_toevaluate_list
        self.rowid = None
        self.row_df = None
        self.word_lod = [] # valid for the whole columns
        self.add_row_df(rowid,row_data)
        # list of lookup operations done with predefine ratio
        # self.matched_src_list = []
        self.add_word_lod(chars_tostrip=chars_tostrip\
                          ,wordindex_group_dict=wordindex_group_dict\
                          ,wordindex_simple=wordindex_simple
                          ,baseword_list=baseword_list\
                          ,replace_dict=replace_dict)
        #self.remove_dup_word_lod()



    def add_row_df(self,rowid,row_data):
        self.rowid = rowid
        # if fieldname_to_evaluate passed by client then
        # add only those fields into self.row_df
        #deprecated on 20181105 if self.fieldname_toevaluate_list is not None:
        if len(self.fieldname_toevaluate_list) > 0:
            col_list = (list(row_data.keys()))
            for col in col_list:
                if col not in self.fieldname_toevaluate_list:
                    row_data = row_data.drop(col)        
        self.row_df = row_data

    def remove_dup_word_lod(self):
        _word_lod = []
        for _rd in self.word_lod:
            if _rd["val"] not in [x["val"] for x in _word_lod]:
                _word_lod.append(_rd)
            # if _rd["val"] in [x["val"] for x in _word_lod]:
            #     _gi =
            #     if _rd["gi"] in [1] and _rd["wi"] in [0]:
            #         _word_lod.append(_rd)

        self.word_lod = _word_lod
        # print("-------------")
        # for res in _word_lod:
        #     print(res)
        # quit()


    def add_word_lod(self,chars_tostrip=None\
                     ,wordindex_group_dict=None\
                     ,wordindex_simple=None\
                     ,baseword_list=None\
                     ,replace_dict=None):
        """normalise any words from any columns into key value"""
        col_list = (list(self.row_df.keys()))
        row_list = []
        # populate the list with any value from each fields
        for k,v in self.row_df.items():
            #print("debug row_df",k,v)
            # if k not in self.fieldname_toevaluate_list:
            #     print("a field skipped")
            #     continue
            # display what trg's rowid/value being scanned
            #print('scanning src data for cell ({} {}) {}:{}...'.format(col_list.index(k),self.rowid,k,v))

            # go to next cell if current pointer is null
            if pd.isnull(v) == True or k == "_rownum":
                continue
            #tlist = app_adhoc.config.clas.basepath_space_clas_list
            rc = Row_Column(v,chars_tostrip=chars_tostrip\
                             ,wordindex_group_dict=wordindex_group_dict\
                             ,wordindex_simple=wordindex_simple\
                             ,baseword_list=baseword_list\
                             ,replace_dict=replace_dict)

            for rc_dict in rc.word_col_lod:
                row_dict = {}
                row_dict["idx"] = len(self.word_lod)
                row_dict["col_idx"] = col_list.index(k)
                row_dict["col"] = k
                for k_rc,v_rc in rc_dict.items():
                    row_dict[k_rc] = v_rc
                self.word_lod.append(row_dict)

    def debug_object(self):
        print("*************** debug ***************")
        print("clas: Row_Target")
        print("row_id: {}".format(self.rowid))
        print("word_lod:")
        for item in self.word_lod:
            print(" ",item)
        print("matched src list:")
        print("----------------")
        for rs in self.matched_src_list:
            print("rowid:{}, weighted score:{}, penalty:{}".format(rs.rowid,round(rs.score_weighted,1),round(rs.penalty,1)))
        print("*************************************")

    def get_debug_object_list(self):
        _list = []
        _list.append("*************** debug ***************")
        _list.append("clas: Row_Target")
        _list.append("row_id: {}".format(self.rowid))
        _list.append("word_lod:")
        for item in self.word_lod:
            _wlitem = " " + str(item)
            _list.append(_wlitem)
        _list.append("matched src list:")
        _list.append("----------------")
        for rs in self.matched_src_list:
            _rrmatchsrc = "rowid:{}, weighted score:{}, penalty:{}".format(rs.rowid,round(rs.score_weighted,1),round(rs.penalty,1))
            _list.append(_rrmatchsrc)
        _list.append("*************************************")
        return _list




class Row_Target(Row):
    def __init__(self,rowid,row_df\
                 ,chars_tostrip=None\
                 ,fieldname_toevaluate_list=None\
                 ,wordindex_group_dict=None\
                 ,wordindex_simple=None\
                 ,baseword_list=None\
                 ,replace_dict=None):

        Row.__init__(self,rowid,row_df\
                         ,chars_tostrip=chars_tostrip\
                         ,fieldname_toevaluate_list=fieldname_toevaluate_list\
                         ,wordindex_group_dict=wordindex_group_dict\
                         ,wordindex_simple=wordindex_simple\
                         ,baseword_list=baseword_list\
                         ,replace_dict=replace_dict)
        self.matched_src_list = []


class Row_Source(Row):
    def __init__(self,rowid,row_df\
                     ,chars_tostrip=None\
                     ,fieldname_toevaluate_list=None\
                     ,wordindex_group_dict=None\
                     ,wordindex_simple=None\
                     ,baseword_list=None\
                     ,replace_dict=None):
                     # ,fuzzratio_min=None\
                     # ,penalty_rate=None):
        Row.__init__(self,rowid,row_df\
                         ,chars_tostrip=chars_tostrip\
                         ,fieldname_toevaluate_list=fieldname_toevaluate_list\
                         ,wordindex_group_dict=wordindex_group_dict\
                         ,wordindex_simple=wordindex_simple\
                         ,baseword_list=baseword_list\
                         ,replace_dict=replace_dict)
        #self.clas_type = clas_type
        #self.fuzzratio_min = fuzzratio_min
        self.word_matched_lod = []
        self.scoretype_list = []
        self.score_weighted = 0
        self.word_mismatched_lod = []
        self.penalty = 0


    def set_weighted_score(self):
        pass
        # for item in self.word_matched_lod:
        #     for _scr in self.scoretype_list:
        #         self.score_weighted = self.score_weighted + item[_scr]


    def add_word_matched_lod(self,rt,fuzzratio_min=None\
                             ,penalty_rate=None\
                             ,penalty_digit_rate=None\
                             ,trg_baseword_rate=None\
                             ,src_baseword_rate=None\
                             ,baseword_matched_rate=None\
                             ,word_common_list=None\
                             ,word_common_rate=None\
                             ,trg_weight_colidx=None\
                             ,trg_weight_groupidx=None\
                             ,trg_weight_wordidx=None\
                             ,src_weight_groupidx=None\
                             ,src_weight_wordidx=None):
        _scoretype_list = []
        # create a list of matched word for fuzzy ratio testing
        for _rt_word_dict in rt.word_lod:
            #rr_debug = self.word_lod[:]
            for _rs_word_dict in self.word_lod:
                wm_dict = {}
                wm_dict["idx_trg"] = _rt_word_dict["idx"]
                wm_dict["idx_src"] = _rs_word_dict["idx"]

                fr = self.get_fuzz_ratio(_rt_word_dict["val"],_rs_word_dict["val"])
                # if word_common_list is not None:
                #     #if a word in under word_common_list then decrease the score
                #     if _rs_word_dict["val"] in word_common_list:
                #         fr = fr - (fr * word_common_rate / 100)
                # if _rs_word_dict["val"].isdigit() == True:
                #     #if a word is is_digit() then decrease the score
                #     #in future: should be made outside here!
                #     #           eg. make it higher if other words are matched
                #     fr = fr - (fr * penalty_digit_rate / 100)
                # set weighted average
                if trg_weight_colidx is not None:
                    if _rt_word_dict["col_idx"] < len(trg_weight_colidx):
                        wm_dict["sco_trg_colidx"] = trg_weight_colidx[_rt_word_dict["col_idx"]]
                    else:
                        wm_dict["sco_trg_colidx"] = 1
                    if "sco_trg_colidx" not in self.scoretype_list:
                        self.scoretype_list.append("sco_trg_colidx")


                if trg_weight_groupidx is not None:
                    if _rt_word_dict["gi"] < len(trg_weight_groupidx):
                        wm_dict["sco_trg_gi"] = trg_weight_groupidx[_rt_word_dict["gi"]]
                    else:
                        wm_dict["sco_trg_gi"] = 1
                    if "sco_trg_gi" not in self.scoretype_list:
                        self.scoretype_list.append("sco_trg_gi")

                if trg_weight_wordidx is not None:
                    if _rt_word_dict["wi"] < len(trg_weight_wordidx):
                        wm_dict["sco_trg_wi"] = trg_weight_wordidx[_rt_word_dict["wi"]]
                    else:
                        wm_dict["sco_trg_wi"] = 1
                    if "sco_trg_wi" not in self.scoretype_list:
                        self.scoretype_list.append("sco_trg_wi")

                if src_weight_groupidx is not None:
                    if _rs_word_dict["gi"] < len(src_weight_groupidx):
                        wm_dict["sco_src_gi"] = src_weight_groupidx[_rs_word_dict["gi"]]
                    else:
                        wm_dict["sco_src_gi"] = 1
                    if "sco_src_gi" not in self.scoretype_list:
                        self.scoretype_list.append("sco_src_gi")


                if src_weight_wordidx is not None:
                    if _rs_word_dict["wi"] < len(src_weight_wordidx):
                        wm_dict["sco_src_wi"] = src_weight_wordidx[_rs_word_dict["wi"]]
                    else:
                        wm_dict["sco_src_wi"] = 1
                    if "sco_src_wi" not in self.scoretype_list:
                        self.scoretype_list.append("sco_src_wi")
                #
                # define score for trg_has_bw
                if trg_baseword_rate is not None:
                    if _rt_word_dict["bw"] is not None:
                        wm_dict["sco_trg_has_bw"] = trg_baseword_rate
                    else:
                        wm_dict["sco_trg_has_bw"] = 1
                    if "sco_trg_has_bw" not in self.scoretype_list:
                        self.scoretype_list.append("sco_trg_has_bw")

                # define score for src_has_bw
                if src_baseword_rate is not None:
                    if _rs_word_dict["bw"] is not None:
                        wm_dict["sco_src_has_bw"] = src_baseword_rate
                    else:
                        wm_dict["sco_src_has_bw"] = 1
                    if "sco_src_has_bw" not in self.scoretype_list:
                        self.scoretype_list.append("sco_src_has_bw")

                # define score for bw match
                # if self.rowid in [2581]:
                #     print(self.rowid,_rt_word_dict["bw"],_rs_word_dict["bw"])
                wm_dict["sco_bw_matched"] = 1
                if "sco_bw_matched" not in self.scoretype_list:
                    self.scoretype_list.append("sco_bw_matched")
                if baseword_matched_rate is not None:
                    if _rs_word_dict["bw"] is not None:
                        if _rt_word_dict["bw"] == _rs_word_dict["bw"]:
                            wm_dict["sco_bw_matched"] = baseword_matched_rate


                wm_dict["fr"] = fr

                if fr < fuzzratio_min:
                    if penalty_rate is not None:
                        # scorep = (100-fr)* wm_dict["sco_trg_colidx"]\
                        #         * wm_dict["sco_trg_gi"]\
                        #         * wm_dict["sco_trg_wi"]\
                        #         * wm_dict["sco_src_gi"]\
                        #         * wm_dict["sco_src_wi"]\
                        #         * wm_dict["sco_trg_has_bw"]\
                        #         * wm_dict["sco_src_has_bw"]\
                        #         * wm_dict["sco_bw_matched"]/10000000000*penalty_rate
                        # self.penalty = self.penalty + scorep
                        self.penalty = self.penalty + ((100-fr) * penalty_rate/100)




                if fr >= fuzzratio_min:
                    if word_common_list is not None:
                        #if a word in under word_common_list then decrease the score
                        if _rs_word_dict["val"] in word_common_list:
                            fr = fr - (fr * word_common_rate / 100)
                    if _rs_word_dict["val"].isdigit() == True:
                        #if a word is is_digit() then decrease the score
                        #in future: should be made outside here!
                        #           eg. make it higher if other words are matched
                        fr = fr - (fr * penalty_digit_rate / 100)

                    # set boosted score by creating relationship by user scores
                    score = fr * wm_dict["sco_trg_colidx"]\
                            * wm_dict["sco_trg_gi"]\
                            * wm_dict["sco_trg_wi"]\
                            * wm_dict["sco_src_gi"]\
                            * wm_dict["sco_src_wi"]\
                            * wm_dict["sco_trg_has_bw"]\
                            * wm_dict["sco_src_has_bw"]\
                            * wm_dict["sco_bw_matched"]/100 #this divident has been an issue for _proc_manuf
                    # print('-----------debug word score---------------')
                    # # print('\t','trg_rowid: {} trg_idx: {} src_word: {} \
                    # #        src_rowid: {} src_idx: {} src_word: {}'.format(rt.rowid,rt.word_lod["idx"],_rt_word_dict["val"],self.rowid,self.word_lod["idx"],_rs_word_dict["val"]))
                    # print('\t','score: ',score)
                    # print('\t','fr: ',fr)
                    # print('\t','wm_dict["sco_trg_colidx"]',wm_dict["sco_trg_colidx"])
                    # print('\t','wm_dict["sco_trg_gi"]',wm_dict["sco_trg_gi"])
                    # print('\t','wm_dict["sco_trg_wi"]',wm_dict["sco_trg_wi"])
                    # print('\t','wm_dict["sco_src_gi"]',wm_dict["sco_src_gi"])
                    # print('\t','wm_dict["sco_src_wi"]',wm_dict["sco_src_wi"])
                    # print('\t','wm_dict["sco_src_has_bw"]',wm_dict["sco_src_has_bw"])
                    # print('\t','wm_dict["sco_src_has_bw"]',wm_dict["sco_src_has_bw"])
                    # print('\t','wm_dict["sco_bw_matched"]',wm_dict["sco_bw_matched"])
                    # print('-----------end debug---------------')

                    # for k,v in wm_dict.items():
                    #     print('\t',fr,k,v)
                    # if self.rowid in [2544,741,2903,2581]:
                    #     print("id {} score {} details {}".format(self.rowid,score,wm_dict))
                    self.score_weighted =  self.score_weighted + score

                    # try:
                    # # too lame if _rt_word_dict["col_idx"] == 0\
                    # #     and _rt_word_dict["wi"] == 0\
                    # #     and _rs_word_dict["gi"] == 1\
                    # #     and _rs_word_dict["wi"] == 0:
                    # #     print(_rt_word_dict)
                    # #     print(_rs_word_dict)
                    #     bs = trg_weight_colidx[_rt_word_dict["col_idx"]]\
                    #        * trg_weight_groupidx[_rt_word_dict["gi"]]\
                    #        * trg_weight_wordidx[_rt_word_dict["wi"]]\
                    #        * src_weight_groupidx[_rs_word_dict["gi"]]\
                    #        * src_weight_wordidx[_rs_word_dict["wi"]]
                    #     self.score_weighted = self.score_weighted + bs
                    #     # print("bs:{}".format(bs))
                    # except:
                    #     pass
                    self.word_matched_lod.append(wm_dict)


    def get_fuzz_ratio(self,search_entry,src_entry):
        # a few of cleansing/adjustments to boost the matching ratio
        try:
            search_entry_lc = search_entry.lower()
            src_entry_lc = src_entry.lower()
            # get the ratio
            ratio = fuzz.ratio(search_entry_lc, src_entry_lc)
            # if ratio >89:
            #     print(ratio,search_entry_lc,src_entry_lc)
            return ratio
        except:
            #print(src_entry_lc)
            return 0

    def debug_matched_word_lod(self):
        for item in self.word_matched_lod:
            print(item)

    def debug_word_lod(self):
        for item in self.word_lod:
            print(item)

class HILookup:
    def __init__(self,src_df, trg_df, numof_output=3):
        # future: an internal, balanced config should be used
        #         to assign default values of these attributes

        self.chars_tostrip = None #';:[-/()\{\}<>*]'
        self.fuzzratio_min = 90
        self.penalty_rate = 10
        self.penalty_digit_rate = 10
        self.word_common_list = None
        self.word_common_rate = 60
        self.matched_score_min = 50
        self.trg_weight_colidx = [1]
        self.trg_weight_groupidx = [1]
        self.trg_weight_wordidx = [1]
        self.src_weight_groupidx = [1]
        self.src_weight_wordidx = [1]
        self.src_df = src_df
        self.src_fieldname_toevaluate_list = []
        self.src_wordindex_group_dict = None
        self.src_wordindex_simple = None
        self.src_baseword_list = None
        self.src_baseword_rate = 1
        self.src_replace_dict = None
        self.trg_df = trg_df
        self.trg_fieldname_toevaluate_list = []
        self.trg_wordindex_group_dict = None
        self.trg_wordindex_simple = None
        self.trg_baseword_list = None
        self.trg_baseword_rate = 10
        self.trg_replace_dict = None
        self.trg_df_matched = None
        self.baseword_matched_rate = 1
        self.src_list = []
        self.src_dumped_list = [] # to control rs dump to one only at rs object creation run time
        self.trg_matching_list = []
        self.numof_output = numof_output
        self.is_debug_mode = False
        self.will_dump_object = False
        self.dump_directory = None
        self.trg_rownum_todebug_list = []
        self.src_rownum_todebug_list = []
        self.init_dict = {}
        for k,v in self.__dict__.items():
            if k not in ['trg_df','src_df']:
                self.init_dict[k] = v



    def validate_user_input(self):

        #--------------------
        # validate user inputs
        #--------------------

        # provide error/warning if user provide new attributes
        for k,v in self.__dict__.items():
            if k not in ['trg_df','src_df']:
                #after_init_dict[k] = v
                if k not in self.init_dict:
                    print("Error!!! {} is not in flookup object! Exiting...".format(k))
                    quit()

        # provide warning for any null value. only in debug mode
        if self.is_debug_mode:
            for k,v in self.__dict__.items():
                if v is None:
                    print("Warning!!! {} assigned to None".format(k))

        # provide info for changed value
        # for k,v in self.__dict__.items():
        #     if k not in ['trg_df','src_df']:
        #         if v != self.init_dict[k]:
        #             print("Info! {} assigned to {} default {}".format(k,v,self.init_dict[k]))


    def hilookup(self):
        # print(list(self.trg_df))
        # print(list(self.src_df))
        #from itertools import repeat
        # validate user inputs
        self.validate_user_input()


        # add the src list
        self.add_src_list()

        row_trg_list = []
        src_list = self.src_list[:]


        # define trg row to process
        # if debug required then read from the param
        # otherwise process all of the target row
        for (rowid_trg,row_trg) in self.trg_df.iterrows():
            #pdrow = PandasSeries(rowid_trg,row_trg)
            pdrow = ProcessUnit(rowid_trg,row_trg,src_list)
            if len(self.trg_rownum_todebug_list) > 0:
                if rowid_trg in self.trg_rownum_todebug_list:
                    row_trg_list.append(pdrow)
                    continue
            else:
                row_trg_list.append(pdrow)

        numof_core_touse = (multiprocessing.cpu_count() * 2) - 1
        # with Pool(processes=numof_core_touse) as pool:
            #p = pool.map(self.scan_src_row,row_trg_list)
        # for res in p:
        #     if res is not None:
        #         self.trg_matching_list.append(res)

        p = multiprocessing.Pool()
        start = time.time()

        for res in p.imap(self.scan_src_row,row_trg_list):
            if res is not None:
                self.trg_matching_list.append(res)
                    #print("{} (Time elapsed: {}s)".format(res, int(time.time() - start)))



        # else:
        #     src_list = self.src_list[:]
        #     for (rowid_trg,row_trg) in self.trg_df.iterrows():
        #         #pdrow = PandasSeries(rowid_trg,row_trg)
        #
        #
        #         # is it debug mode? if yes scan specific trg row only
        #         # otherwiser scann all trg rows
        #
        #         if self.is_debug_mode:
        #             if rowid_trg in self.trg_rownum_todebug_list:
        #                 pdrow = ProcessUnit(rowid_trg,row_trg,src_list)
        #                 res = self.scan_src_row(pdrow)
        #                 if res is not None:
        #                     self.trg_matching_list.append(res)


                # else:
                #     res = self.scan_src_row(pdrow)
                #     if res is not None:
                #         self.trg_matching_list.append(res)

    def add_src_list(self):
        """ BUGGY """
        for (rowid_src,row_src) in self.src_df.iterrows():
        # src_baseword_list=["\\Classifications\\Space Class Current"]
        # src_replace_dict = None #{"\\Classifications\\Space Class Current":""}

            rs = Row_Source(rowid_src,row_src\
                         ,chars_tostrip=self.chars_tostrip\
                         ,fieldname_toevaluate_list=self.src_fieldname_toevaluate_list\
                         ,wordindex_group_dict=self.src_wordindex_group_dict\
                         ,wordindex_simple=self.src_wordindex_simple\
                         ,baseword_list=self.src_baseword_list\
                         ,replace_dict=self.src_replace_dict)
            self.src_list.append(rs)


    def scan_src_row(self,pdrow):
        rowid_trg = pdrow.rowid
        row_trg = pdrow.row_data

        # print(rowid_trg)
        # print(multiprocessing.current_process())

        rt = Row_Target(rowid_trg,row_trg\
                      ,chars_tostrip=self.chars_tostrip\
                      ,fieldname_toevaluate_list=self.trg_fieldname_toevaluate_list\
                      ,wordindex_group_dict=self.trg_wordindex_group_dict\
                      ,wordindex_simple=self.trg_wordindex_simple\
                      ,baseword_list=self.trg_baseword_list\
                      ,replace_dict=self.trg_replace_dict)
        # # display what trg's rowid/value being scanned
        # print('looking up src data for cell ({} {})...'.format(" ".join(rt.fieldname_toevaluate_list),rt.rowid))
        if self.is_debug_mode:
            if self.will_dump_object and len(self.trg_rownum_todebug_list) > 0:
                if rowid_trg in self.trg_rownum_todebug_list:
                    _dtrg = Dump(self.dump_directory,rt.word_lod,"words_trg(" + str(rowid_trg) + ")")


        for rs in pdrow.src_list:


        # for (rowid_src,row_src) in self.src_df.iterrows():
        #     rs = Row_Source(rowid_src,row_src\
        #                  ,chars_tostrip=self.chars_tostrip\
        #                  ,fieldname_toevaluate_list=self.src_fieldname_toevaluate_list\
        #                  ,wordindex_group_dict=self.src_wordindex_group_dict\
        #                  ,wordindex_simple=self.src_wordindex_simple\
        #                  ,baseword_list=self.src_baseword_list\
        #                  ,replace_dict=self.src_replace_dict)
            # dump rs
            if self.is_debug_mode:
                if self.will_dump_object and len(self.trg_rownum_todebug_list) > 0:
                    if rt.rowid in self.trg_rownum_todebug_list:
                        if rs.rowid in self.src_rownum_todebug_list:
                            _d = Dump(self.dump_directory,rs.word_lod,"words_src({})".format(rs.rowid))


            rs.add_word_matched_lod(rt,fuzzratio_min=self.fuzzratio_min\
                                    ,penalty_rate=self.penalty_rate\
                                    ,penalty_digit_rate=self.penalty_digit_rate\
                                    ,trg_baseword_rate=self.trg_baseword_rate\
                                    ,src_baseword_rate=self.src_baseword_rate\
                                    ,baseword_matched_rate=self.baseword_matched_rate\
                                    ,word_common_list=self.word_common_list\
                                    ,word_common_rate=self.word_common_rate\
                                    ,trg_weight_colidx=self.trg_weight_colidx\
                                    ,trg_weight_groupidx=self.trg_weight_groupidx\
                                    ,trg_weight_wordidx=self.trg_weight_wordidx\
                                    ,src_weight_groupidx=self.src_weight_groupidx\
                                    ,src_weight_wordidx=self.src_weight_wordidx)

            if len(rs.word_matched_lod) > 0:
                # print("length:",len(rs.word_matched_lod))
                rs.set_weighted_score()
                #print(rs.score_weighted)
                if rs.score_weighted >= self.matched_score_min:
                    rt.matched_src_list.append(rs)
                    #print(rs.rowid)



            # dump word_matched_lod
            if self.will_dump_object and len(self.trg_rownum_todebug_list) > 0:
                if rt.rowid in self.trg_rownum_todebug_list:
                    if rs.rowid in self.src_rownum_todebug_list:
                        x = Dump(self.dump_directory,rs.word_matched_lod,"idxmatchedlod({},{})".format(rt.rowid,rs.rowid))
                        pass

        # sort the rt.matched_src_list
        if len(rt.matched_src_list) > 1:
            rt.matched_src_list = sorted(rt.matched_src_list\
                                        ,key=lambda rowsrc_: rowsrc_.score_weighted-rowsrc_.penalty\
                                        ,reverse=True)
        # delete any item from index 3
        if len(rt.matched_src_list) > self.numof_output:
            del rt.matched_src_list[self.numof_output:len(rt.matched_src_list)]


        # the best point to debug trg obj, if asked
        if self.is_debug_mode:
            # rt.debug_object()
            if self.will_dump_object and len(self.trg_rownum_todebug_list) > 0:
                if rowid_trg in self.trg_rownum_todebug_list:
                    _otrg = Dump(self.dump_directory,rt.get_debug_object_list(),"obj_trg(" + str(rowid_trg) + ")")


        # return row target object if the object has at least 1 matching with src
        if len(rt.matched_src_list) > 0:
            return rt



class Dump:
    """plain data dump"""
    def __init__(self,dirname,data, name=None):
        self.totext(dirname,data,name)
    def totext(self,dirname,data,name):
        timestamp = str(datetime.now())
        timestamp = re.sub("[-:.]","",timestamp)
        if name is not None:
            filename = "dump_" + name
        else:
            filename = "dump"
        path = os.path.join(dirname,filename + timestamp + ".txt")
        try:
            file = open(path,"w")
            if type(data) is list or type(data) is tuple:
                for row in data:
                    try:
                        file.write(str(row)+ '\n') #
                    except Exception as e:
                        print('warning! {} with reference to writing row {}.'.format(e,row))
            else:
                file.write(str(data))
        except Exception as e:
            print('warning! {} with reference to dumping object.'.format(e))
            pass
