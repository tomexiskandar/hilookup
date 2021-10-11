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
                     ,char_tosplit_alphanumeric=None\
                     ,replace_dict=None):
        self.word_group_list = []
        self.word_col_list = []
        self.word_col_lod = []
        self.baseword = None
        self.split_words(value\
                         ,chars_tostrip=chars_tostrip\
                         ,wordindex_group_dict=wordindex_group_dict\
                         ,wordindex_simple=wordindex_simple\
                         ,baseword_list=baseword_list\
                         ,char_tosplit_alphanumeric=char_tosplit_alphanumeric\
                         ,replace_dict=replace_dict)


    def split_words(self,value\
                        ,chars_tostrip=None\
                        ,wordindex_group_dict=None\
                        ,wordindex_simple=None\
                        ,baseword_list=None\
                        ,char_tosplit_alphanumeric=None\
                        ,replace_dict=None):
        r"""create a dictionary that holds splitted words
        this list will be a split of seperator"""
        # convert non string value as string
        if type(value) != str:
            value = str(value)
        #replace any untested chars
        if chars_tostrip is not None:
            for ch in chars_tostrip:
                value = value.replace(ch,"").strip()

        if baseword_list is not None:
            baseword = self.get_filtered_baseword(value,baseword_list)
            if baseword is not None:
                self.baseword = baseword
            else:
                self.baseword = None


        # truncate word(s) if required
        # if replace_dict is not None:
        #     value = self.replace_word(value,replace_dict)
        # if replace_dict is not None:
        #     value = self.replace_whole_word(value,replace_dict)

        if char_tosplit_alphanumeric is not None:
            value = self.split_alphanumeric(value,char_tosplit_alphanumeric)

        ####################
        # take the opportunity to update here
        if replace_dict is not None:
            value = self.replace_whole_word(value, replace_dict)
            

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
            # if value =='MAA 400 M M':
            #     print(_dict)
            #     print(wg_item)
            self.word_col_lod.append(_dict)
            # add the splitted words
            if len(wg_item.split()) > 1:
                self.split_simple_words(si,wg_item,wordindex_simple,replace_dict)
                # if value =='MAA 4000 M M':
                #     for row in self.word_col_lod:
                #         print(row)

    def split_group_words(self,value,separator\
                            ,wordindex_group_dict=None):

        if len(self.word_group_list) == 0:
            if wordindex_group_dict[separator] == 'right-to-left':
                _value_list = reversed(value.split(separator))
            else:
                _value_list = value.split(separator)
            # strip any None member
            _value_list = list(filter(None,_value_list))

            for item in _value_list:
                #_list.append(item)
                if item not in self.word_group_list:
                    self.word_group_list.append(item.strip())
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
            self.word_group_list = _list_temp


    def split_simple_words(self,gi,value,wordindex_simple,replace_dict):
        if wordindex_simple is None or wordindex_simple=='right-to-left':
            value = list(reversed(value.split()))
        else:
            value = value.split()
        i = 1
        for item in value:
            # ####################
            # # take the opportunity to update here
            # print(i, item)
            # item = self.replace_whole_word(item,replace_dict)
            self.word_col_list.append(item)
            _dict = {}
            _dict["gi"] = gi
            _dict["wi"] = i ##this is the problem!!!! deprecated value.index(item)+1
            _dict["val"] = item.strip()
            _dict["bw"] = self.baseword
            i+=1

            self.word_col_lod.append(_dict)


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
            insensitive_k = re.compile(re.escape(k), re.IGNORECASE)
            value = insensitive_k.sub(v,value)
        return insensitive_k.sub(v,value)


    def replace_whole_word(self,value,replace_dict=None):
        for k,v in replace_dict.items():
            wholeword_regex = r"\b" + re.escape(k) + r"\b"
            insensitive_k = re.compile(wholeword_regex)
            value = insensitive_k.sub(v,value)
        return value


    def split_alphanumeric(self,value,separator):
        _new_value = []
        prev_char = ''
        for i in value:
            if (prev_char.isalpha() and i.isnumeric()) or (i.isalpha() and prev_char.isnumeric()):
                _new_value.append(separator)
                _new_value.append(i)
            else:
                _new_value.append(i)
            prev_char = i
        return ''.join(_new_value)


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
                     ,char_tosplit_alphanumeric=None\
                     ,replace_dict=None):
        self.fieldname_toevaluate_list = fieldname_toevaluate_list
        self.rowid = None
        self.row_df = None
        self.word_lod = []
        self.reindex_row_df(rowid,row_data)
        self.add_word_lod(chars_tostrip=chars_tostrip\
                          ,wordindex_group_dict=wordindex_group_dict\
                          ,wordindex_simple=wordindex_simple
                          ,baseword_list=baseword_list\
                          ,char_tosplit_alphanumeric=char_tosplit_alphanumeric\
                          ,replace_dict=replace_dict)

    def gen_word_lod(self):
        for dic in self.word_lod:
            yield dic

    def get_wordlod_max_index(self,key):
        # return maximum or the highest index for a given key
        max = 0
        for row in self.word_lod:
            if row[key] > max:
                max = row[key]
        return max

    def reindex_row_df(self,rowid,row_data):
        self.rowid = rowid
        if len(self.fieldname_toevaluate_list) > 0:
            self.row_df = row_data.reindex(self.fieldname_toevaluate_list)
        else:
            self.row_df = row_data



    def remove_dup_word_lod(self):
        _word_lod = []
        for _rd in self.word_lod:
            if _rd["val"] not in [x["val"] for x in _word_lod]:
                _word_lod.append(_rd)
        self.word_lod = _word_lod


    def add_word_lod(self,chars_tostrip=None\
                     ,wordindex_group_dict=None\
                     ,wordindex_simple=None\
                     ,baseword_list=None\
                     ,char_tosplit_alphanumeric=None\
                     ,replace_dict=None):
        """normalise any words from any columns into key value"""
        col_list = (list(self.row_df.keys()))
        row_list = []
        # populate the list with any value from each fields
        for k,v in self.row_df.items():
            # deprecated. row_df now has been reindexed at reindex_row_df()
            # if k not in self.fieldname_toevaluate_list:
            #     continue

            # go to next cell if current pointer is null or user speific column
            if pd.isnull(v) == True or k == "_rownum":
                continue
            rc = Row_Column(v,chars_tostrip=chars_tostrip\
                             ,wordindex_group_dict=wordindex_group_dict\
                             ,wordindex_simple=wordindex_simple\
                             ,baseword_list=baseword_list\
                             ,char_tosplit_alphanumeric=char_tosplit_alphanumeric\
                             ,replace_dict=replace_dict)

            for rc_dict in rc.word_col_lod:
                row_dict = {}
                row_dict["idx"] = len(self.word_lod)
                row_dict["ci"] = col_list.index(k)
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
                 ,char_tosplit_alphanumeric=None\
                 ,replace_dict=None):

        Row.__init__(self,rowid,row_df\
                         ,chars_tostrip=chars_tostrip\
                         ,fieldname_toevaluate_list=fieldname_toevaluate_list\
                         ,wordindex_group_dict=wordindex_group_dict\
                         ,wordindex_simple=wordindex_simple\
                         ,baseword_list=baseword_list\
                         ,char_tosplit_alphanumeric=char_tosplit_alphanumeric\
                         ,replace_dict=replace_dict)
        self.matched_src_list = []
        self.base_word_matched_lod = []
        self.base_score_weighted = 0
        self.base_word_mismatched_lod = []
        self.base_penalty = 0
        self.scans = 0


    def get_fuzz_ratio(self,search_entry,src_entry):
        # a few of cleansing/adjustments to boost the matching ratio
        try:
            search_entry_lc = search_entry.lower()
            src_entry_lc = src_entry.lower()
            return fuzz.ratio(search_entry_lc, src_entry_lc)
        except:
            return 0

    def scan_words_and_score_forbase(self,rt,fuzzratio_min=None\
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
                             ,src_weight_colidx=None\
                             ,src_weight_groupidx=None\
                             ,src_weight_wordidx=None\
                             ,is_debug_mode=None):

        # define min and max for weighted score
        score_weighted_min = fuzzratio_min * min(trg_weight_colidx)\
                             * min(trg_weight_groupidx)\
                             * min(trg_weight_wordidx)\
                             * min(src_weight_colidx)\
                             * min(src_weight_groupidx)\
                             * min(src_weight_wordidx)\
                             * 1 #represent the min for trg_baseword_rate,src_baseword_rate and baseword_matched_rate
        score_weighted_max = 100 * max(trg_weight_colidx)\
                             * max(trg_weight_groupidx)\
                             * max(trg_weight_wordidx)\
                             * max(src_weight_colidx)\
                             * max(src_weight_groupidx)\
                             * max(src_weight_wordidx)\
                             * trg_baseword_rate\
                             * src_baseword_rate\
                             * baseword_matched_rate

        # create a list of matched word for fuzzy ratio testing
        for _rt_word_dict in rt.word_lod:

            # used generator to testing speed. for _rs_word_dict in self.word_lod:
            for _rs_word_dict in self.gen_word_lod():

                wm_dict = {}
                wm_dict["idx_trg"] = _rt_word_dict["idx"]
                wm_dict["idx_src"] = _rs_word_dict["idx"]

                fr = self.get_fuzz_ratio(_rt_word_dict["val"],_rs_word_dict["val"])

                # set weighted average
                if _rt_word_dict["ci"] < len(trg_weight_colidx):
                    wm_dict["sco_trg_colidx"] = trg_weight_colidx[_rt_word_dict["ci"]]
                else:
                    wm_dict["sco_trg_colidx"] = 1

                if _rt_word_dict["gi"] < len(trg_weight_groupidx):
                    wm_dict["sco_trg_gi"] = trg_weight_groupidx[_rt_word_dict["gi"]]
                else:
                    wm_dict["sco_trg_gi"] = 1


                if _rt_word_dict["wi"] < len(trg_weight_wordidx):
                    wm_dict["sco_trg_wi"] = trg_weight_wordidx[_rt_word_dict["wi"]]
                else:
                    wm_dict["sco_trg_wi"] = 1


                if _rs_word_dict["ci"] < len(src_weight_colidx):
                    wm_dict["sco_src_colidx"] = src_weight_colidx[_rs_word_dict["ci"]]
                else:
                    wm_dict["sco_src_colidx"] = 1

                if _rs_word_dict["gi"] < len(src_weight_groupidx):
                    wm_dict["sco_src_gi"] = src_weight_groupidx[_rs_word_dict["gi"]]
                else:
                    wm_dict["sco_src_gi"] = 1


                if _rs_word_dict["wi"] < len(src_weight_wordidx):
                    wm_dict["sco_src_wi"] = src_weight_wordidx[_rs_word_dict["wi"]]
                else:
                    wm_dict["sco_src_wi"] = 1

                # define score for trg_has_bw
                if _rt_word_dict["bw"] is not None:
                    wm_dict["sco_trg_has_bw"] = trg_baseword_rate
                else:
                    wm_dict["sco_trg_has_bw"] = 1

                # define score for src_has_bw
                if _rs_word_dict["bw"] is not None:
                    wm_dict["sco_src_has_bw"] = src_baseword_rate
                else:
                    wm_dict["sco_src_has_bw"] = 1

                # define score for bw match
                wm_dict["sco_bw_matched"] = 1
                if baseword_matched_rate is not None:
                    if _rs_word_dict["bw"] is not None:
                        if _rt_word_dict["bw"] == _rs_word_dict["bw"]:
                            wm_dict["sco_bw_matched"] = baseword_matched_rate


                wm_dict["fr"] = fr
                if is_debug_mode == True:
                    wm_dict['val_trg'] = _rt_word_dict['val']
                    wm_dict['val_src'] = _rs_word_dict['val']

                if fr < fuzzratio_min:
                    if penalty_rate is not None:
                        inputp = (100-fr)* wm_dict["sco_trg_colidx"]\
                                * wm_dict["sco_trg_gi"]\
                                * wm_dict["sco_trg_wi"]\
                                * wm_dict["sco_src_colidx"]\
                                * wm_dict["sco_src_gi"]\
                                * wm_dict["sco_src_wi"]\
                                * wm_dict["sco_trg_has_bw"]\
                                * wm_dict["sco_src_has_bw"]\
                                * wm_dict["sco_bw_matched"]

                        scorep_pct =  ((inputp + score_weighted_min)) / (score_weighted_max - score_weighted_min) * (penalty_rate/100)

                        self.base_penalty = self.base_penalty + scorep_pct
                        if is_debug_mode == True:
                            wm_dict["min"] = score_weighted_min
                            wm_dict["max"] = score_weighted_max
                            wm_dict["score_pct"] = scorep_pct
                            self.base_word_mismatched_lod.append(wm_dict)



                #print("fr",fr)
                if fr >= fuzzratio_min:
                    if word_common_list is not None:
                        #if a word in under word_common_list then decrease the score
                        if _rs_word_dict["val"].upper() in word_common_list\
                          or _rt_word_dict["val"].upper() in word_common_list:
                            fr = fr * word_common_rate / 100

                    if penalty_digit_rate is not None:
                        if _rs_word_dict["val"].isdigit() == True or _rt_word_dict["val"].isdigit() == True:
                            fr = fr - (fr * penalty_digit_rate / 100)


                    input = fr * wm_dict["sco_trg_colidx"]\
                            * wm_dict["sco_trg_gi"]\
                            * wm_dict["sco_trg_wi"]\
                            * wm_dict["sco_src_colidx"]\
                            * wm_dict["sco_src_gi"]\
                            * wm_dict["sco_src_wi"]\
                            * wm_dict["sco_trg_has_bw"]\
                            * wm_dict["sco_src_has_bw"]\
                            * wm_dict["sco_bw_matched"]
                    if input < score_weighted_min:
                        score_pct =  input / score_weighted_max * 100
                    else:
                        score_pct =  ((input - score_weighted_min) * 100) / (score_weighted_max - score_weighted_min)

                    self.base_score_weighted =  self.base_score_weighted + score_pct

                    # keep wm_dict to be populated
                    wm_dict["min"] = score_weighted_min
                    wm_dict["max"] = score_weighted_max
                    wm_dict["score_pct"] = score_pct
                    self.base_word_matched_lod.append(wm_dict)


class Row_Source(Row):
    def __init__(self,rowid,row_df\
                     ,chars_tostrip=None\
                     ,fieldname_toevaluate_list=None\
                     ,wordindex_group_dict=None\
                     ,wordindex_simple=None\
                     ,baseword_list=None\
                     ,char_tosplit_alphanumeric=None\
                     ,replace_dict=None):

        Row.__init__(self,rowid,row_df\
                         ,chars_tostrip=chars_tostrip\
                         ,fieldname_toevaluate_list=fieldname_toevaluate_list\
                         ,wordindex_group_dict=wordindex_group_dict\
                         ,wordindex_simple=wordindex_simple\
                         ,baseword_list=baseword_list\
                         ,char_tosplit_alphanumeric=char_tosplit_alphanumeric\
                         ,replace_dict=replace_dict)
        self.word_matched_lod = []
        self.scoretype_list = []
        self.score_weighted = 0
        self.word_mismatched_lod = []
        self.penalty = 0
    


    def scan_words_and_score(self,rt,fuzzratio_min=None\
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
                             ,src_weight_colidx=None\
                             ,src_weight_groupidx=None\
                             ,src_weight_wordidx=None\
                             ,is_debug_mode=None):

        # define min and max for weighted score
        score_weighted_min = fuzzratio_min * min(trg_weight_colidx)\
                             * min(trg_weight_groupidx)\
                             * min(trg_weight_wordidx)\
                             * min(src_weight_colidx)\
                             * min(src_weight_groupidx)\
                             * min(src_weight_wordidx)\
                             * 1 #represent the min for trg_baseword_rate,src_baseword_rate and baseword_matched_rate
        score_weighted_max = 100 * max(trg_weight_colidx)\
                             * max(trg_weight_groupidx)\
                             * max(trg_weight_wordidx)\
                             * max(src_weight_colidx)\
                             * max(src_weight_groupidx)\
                             * max(src_weight_wordidx)\
                             * trg_baseword_rate\
                             * src_baseword_rate\
                             * baseword_matched_rate


        # create a list of matched word for fuzzy ratio testing
        for _rt_word_dict in rt.word_lod:
            #rr_debug = self.word_lod[:]
            rt.scans = rt.scans + len(self.word_lod)
            for _rs_word_dict in self.word_lod:
                #this would slow just count the word_lod rt.scans = rt.scans + 1
                wm_dict = {}
                wm_dict["idx_trg"] = _rt_word_dict["idx"]
                wm_dict["idx_src"] = _rs_word_dict["idx"]

                fr = self.get_fuzz_ratio(_rt_word_dict["val"],_rs_word_dict["val"])

                # set weighted average
                if _rt_word_dict["ci"] < len(trg_weight_colidx):
                    wm_dict["sco_trg_colidx"] = trg_weight_colidx[_rt_word_dict["ci"]]
                else:
                    wm_dict["sco_trg_colidx"] = 1

                if _rt_word_dict["gi"] < len(trg_weight_groupidx):
                    wm_dict["sco_trg_gi"] = trg_weight_groupidx[_rt_word_dict["gi"]]
                else:
                    wm_dict["sco_trg_gi"] = 1

                if _rt_word_dict["wi"] < len(trg_weight_wordidx):
                    wm_dict["sco_trg_wi"] = trg_weight_wordidx[_rt_word_dict["wi"]]
                else:
                    wm_dict["sco_trg_wi"] = 1

                if _rs_word_dict["ci"] < len(src_weight_colidx):
                    wm_dict["sco_src_colidx"] = src_weight_colidx[_rs_word_dict["ci"]]
                else:
                    wm_dict["sco_src_colidx"] = 1

                if _rs_word_dict["gi"] < len(src_weight_groupidx):
                    wm_dict["sco_src_gi"] = src_weight_groupidx[_rs_word_dict["gi"]]
                else:
                    wm_dict["sco_src_gi"] = 1


                if _rs_word_dict["wi"] < len(src_weight_wordidx):
                    wm_dict["sco_src_wi"] = src_weight_wordidx[_rs_word_dict["wi"]]
                else:
                    wm_dict["sco_src_wi"] = 1

                # define score for trg_has_bw
                if _rt_word_dict["bw"] is not None:
                    wm_dict["sco_trg_has_bw"] = trg_baseword_rate
                else:
                    wm_dict["sco_trg_has_bw"] = 1

                # define score for src_has_bw
                if _rs_word_dict["bw"] is not None:
                    wm_dict["sco_src_has_bw"] = src_baseword_rate
                else:
                    wm_dict["sco_src_has_bw"] = 1

                # define score for bw match
                # if self.rowid in [2581]:
                #     print(self.rowid,_rt_word_dict["bw"],_rs_word_dict["bw"])
                wm_dict["sco_bw_matched"] = 1
                if baseword_matched_rate is not None:
                    if _rs_word_dict["bw"] is not None:
                        if _rt_word_dict["bw"] == _rs_word_dict["bw"]:
                            wm_dict["sco_bw_matched"] = baseword_matched_rate


                wm_dict["fr"] = fr
                #if is_debug_mode == True:
                wm_dict['val_trg'] = _rt_word_dict['val']
                wm_dict['val_src'] = _rs_word_dict['val']

                if fr < fuzzratio_min:
                    if penalty_rate is not None:
                        inputp = (100-fr)* wm_dict["sco_trg_colidx"]\
                                * wm_dict["sco_trg_gi"]\
                                * wm_dict["sco_trg_wi"]\
                                * wm_dict["sco_src_colidx"]\
                                * wm_dict["sco_src_gi"]\
                                * wm_dict["sco_src_wi"]\
                                * wm_dict["sco_trg_has_bw"]\
                                * wm_dict["sco_src_has_bw"]\
                                * wm_dict["sco_bw_matched"]
                        # print('inputp {}'.format(inputp))
                        # print('-----------------')
                        scorep_pct =  ((inputp + score_weighted_min)) / (score_weighted_max - score_weighted_min) * (penalty_rate/100)
                        # print('scorep {}'.format(scorep_pct))
                        # scorep_pct =  ((inputp + score_weighted_min)) / (score_weighted_max - score_weighted_min)
                        # print('scorep {}'.format(scorep_pct))
                        self.penalty = self.penalty + scorep_pct
                        if is_debug_mode == True:
                            wm_dict["min"] = score_weighted_min
                            wm_dict["max"] = score_weighted_max
                            wm_dict["score_pct"] = scorep_pct
                            self.word_mismatched_lod.append(wm_dict)




                if fr >= fuzzratio_min:
                    if word_common_list is not None:
                        #if a word in under word_common_list then decrease the score
                        if _rs_word_dict["val"].upper() in word_common_list\
                          or _rt_word_dict["val"].upper() in word_common_list:
                            fr = fr * word_common_rate / 100

                            #if rt.rowid==24 and self.rowid==244:
                                # print('--------')
                                # print(_rs_word_dict["val"])
                                # print(fr)
                                # print('--------')
                    if penalty_digit_rate is not None:
                        #if a word is is_digit() then decrease the score
                        #in future: should be made outside here!
                        #           eg. make it higher if other words are matched
                        if _rs_word_dict["val"].isdigit() == True or _rt_word_dict["val"].isdigit() == True:
                            fr = fr - (fr * penalty_digit_rate / 100)
                            #if rt.rowid==24 and self.rowid==244:
                                # print('----digit----')
                                # print(fr)
                                # print('----digit----')

                    # re set fr in the dict
                    wm_dict["fr"] = fr
                    # set boosted score by creating relationship by user scores

                    input = fr * wm_dict["sco_trg_colidx"]\
                            * wm_dict["sco_trg_gi"]\
                            * wm_dict["sco_trg_wi"]\
                            * wm_dict["sco_src_colidx"]\
                            * wm_dict["sco_src_gi"]\
                            * wm_dict["sco_src_wi"]\
                            * wm_dict["sco_trg_has_bw"]\
                            * wm_dict["sco_src_has_bw"]\
                            * wm_dict["sco_bw_matched"]
                    if input < score_weighted_min:
                        score_pct =  input / score_weighted_max * 100
                    else:
                        score_pct =  ((input - score_weighted_min) * 100) / (score_weighted_max - score_weighted_min)
                    
                    # add up scores
                    self.score_weighted =  self.score_weighted + score_pct

                    # keep wm_dict to be populated
                    wm_dict["min"] = score_weighted_min
                    wm_dict["max"] = score_weighted_max
                    wm_dict["score_pct"] = score_pct
                    self.word_matched_lod.append(wm_dict)



    def get_fuzz_ratio(self,search_entry,src_entry):
        # a few of cleansing/adjustments to boost the matching ratio
        try:
            search_entry_lc = search_entry.lower()
            src_entry_lc = src_entry.lower()
            return fuzz.ratio(search_entry_lc, src_entry_lc)
        except:
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
        self.word_common_list = None
        self.word_common_rate = 51
        self.chars_tostrip = None #';:[-/()\{\}<>*]'
        self.fuzzratio_min = 90
        self.penalty_rate = None
        self.penalty_digit_rate = None
        self.matched_score_min = 90
        self.trg_weight_colidx = [1]
        self.trg_weight_groupidx = [1]
        self.trg_weight_wordidx = [1]
        self.src_weight_colidx = [1]
        self.src_weight_groupidx = [1]
        self.src_weight_wordidx = [1]
        self.src_df = src_df
        self.src_fieldname_toevaluate_list = []
        self.src_wordindex_group_dict = None
        self.src_wordindex_simple = None
        self.src_baseword_list = None
        self.src_baseword_rate = 1
        self.src_char_tosplit_alphanumeric = None
        self.src_replace_dict = None
        self.trg_df = trg_df
        self.trg_fieldname_toevaluate_list = []
        self.trg_wordindex_group_dict = None
        self.trg_wordindex_simple = None
        self.trg_baseword_list = None
        self.trg_baseword_rate = 1
        self.trg_char_tosplit_alphanumeric = None
        self.trg_replace_dict = None
        #self.trg_df_matched = None
        self.baseword_matched_rate = 1
        self.src_list = []
        self.src_dumped_list = [] # to control rs dump to one only at rs object creation run time
        self.trg_processed_cnt = 0
        self.scans = 0
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

        # provide error if user provide column name that does not exist in the target data
        trg_cols = list(self.trg_df)
        for col in self.trg_fieldname_toevaluate_list:
            if col not in trg_cols:
                print("Error!!! {} is not in target dataset! Exiting...".format(col))
                quit()

        # provide error if user provide column name that does not exist in the src data
        src_cols = list(self.src_df)
        for col in self.src_fieldname_toevaluate_list:
            if col not in src_cols:
                print("Error!!! {} is not in source dataset! Exiting...".format(col))
                quit()        

        
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


    def subset_columns(self):
        # target
        if len(self.trg_fieldname_toevaluate_list) > 0:
            self.trg_df = self.trg_df[self.trg_fieldname_toevaluate_list]
        # source
        if len(self.src_fieldname_toevaluate_list) > 0:
            self.src_df = self.src_df[self.src_fieldname_toevaluate_list]


    def hilookup(self):
        # validate user inputs
        # DEPRECATED...call this before this function being called. self.validate_user_input()


        # uppercase the member of commond words
        # so we can avoid to do this in scan_words_and_score()
        if self.word_common_list is not None:
            self.word_common_list = [x.upper() for x in self.word_common_list]




        # check evaluated fields, strip them if not wanted
        #self.subset_columns()
        # add the src list
        self.add_src_list()
        # define trg row to process
        row_trg_list = []
        src_list = self.src_list[:]
        for (rowid_trg,row_trg) in self.trg_df.iterrows():
            pdrow = ProcessUnit(rowid_trg,row_trg,src_list)
            if len(self.trg_rownum_todebug_list) > 0:
                if rowid_trg in self.trg_rownum_todebug_list:
                    row_trg_list.append(pdrow)
                    continue
            else:
                row_trg_list.append(pdrow)
        
        cores_tospare = 0
        cores_touse = 1
        if multiprocessing.cpu_count() > 2:
            cores_tospare = 2
            cores_touse = multiprocessing.cpu_count() - cores_tospare
            
        # deprecated ##########################################
        # use the imap instead so we can emit few info to the caller
        #with Pool(processes=numof_core_touse) as pool:
            #p = pool.map(self.scan_src_row,row_trg_list)
        # for res in p:
        #     if res is not None:
        #         self.trg_matching_list.append(res)
        #######################################################

        
        p = multiprocessing.Pool(processes = cores_touse)
        for res in p.imap(self.scan_src_row,row_trg_list):
            # optional to send out useful info whenever we've processed one target row
            self.trg_processed_cnt = self.trg_processed_cnt + 1
            # send to hi for the numbers of scans done
            try:
                self.scans = self.scans + res.scans
            except:
                pass
            if res is not None:
                self.trg_matching_list.append(res)


    def add_src_list(self):
        for (rowid_src,row_src) in self.src_df.iterrows():
            rs = Row_Source(rowid_src,row_src\
                         ,chars_tostrip=self.chars_tostrip\
                         ,fieldname_toevaluate_list=self.src_fieldname_toevaluate_list\
                         ,wordindex_group_dict=self.src_wordindex_group_dict\
                         ,wordindex_simple=self.src_wordindex_simple\
                         ,baseword_list=self.src_baseword_list\
                         ,char_tosplit_alphanumeric=self.src_char_tosplit_alphanumeric\
                         ,replace_dict=self.src_replace_dict)
            self.src_list.append(rs)


    def scan_src_row(self,pdrow):
        rowid_trg = pdrow.rowid
        row_trg = pdrow.row_data
        rt = Row_Target(rowid_trg,row_trg\
                      ,chars_tostrip=self.chars_tostrip\
                      ,fieldname_toevaluate_list=self.trg_fieldname_toevaluate_list\
                      ,wordindex_group_dict=self.trg_wordindex_group_dict\
                      ,wordindex_simple=self.trg_wordindex_simple\
                      ,baseword_list=self.trg_baseword_list\
                      ,char_tosplit_alphanumeric=self.trg_char_tosplit_alphanumeric\
                      ,replace_dict=self.trg_replace_dict)

        if self.is_debug_mode:
            if self.will_dump_object and len(self.trg_rownum_todebug_list) > 0:
                if rowid_trg in self.trg_rownum_todebug_list:
                    _dtrg = Dump(self.dump_directory,rt.word_lod,"words_trg(" + str(rowid_trg) + ")")


        for rs in pdrow.src_list:

        #deprecated
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
                            _d.totext()


            rs.scan_words_and_score(rt,fuzzratio_min=self.fuzzratio_min\
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
                                    ,src_weight_colidx=self.src_weight_colidx\
                                    ,src_weight_groupidx=self.src_weight_groupidx\
                                    ,src_weight_wordidx=self.src_weight_wordidx\
                                    ,is_debug_mode=self.is_debug_mode)


            if len(rs.word_matched_lod) > 0:
                if self.penalty_rate is not None:
                    score_weighted_total = rs.score_weighted - rs.penalty
                else:
                    score_weighted_total = rs.score_weighted
                if score_weighted_total  >= self.matched_score_min:
                    rt.matched_src_list.append(rs)


            # dump word_matched_lod
            if self.will_dump_object and len(self.trg_rownum_todebug_list) > 0:
                if rt.rowid in self.trg_rownum_todebug_list:
                    if rs.rowid in self.src_rownum_todebug_list:
                        #print(rs.word_matched_lod)

                        x = Dump(self.dump_directory,rs.word_matched_lod,"matchedlod({},{})".format(rt.rowid,rs.rowid))
                        x.tocsv()
                        x = Dump(self.dump_directory,rs.word_mismatched_lod,"mismatchedlod({},{})".format(rt.rowid,rs.rowid))
                        x.tocsv()
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
                    _otrg.totext()

        
        # return row target object if the object has at least 1 matching with src
        if len(rt.matched_src_list) > 0:
            rt.scan_words_and_score_forbase(rt,fuzzratio_min=self.fuzzratio_min\
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
                                    ,src_weight_colidx=self.src_weight_colidx\
                                    ,src_weight_groupidx=self.src_weight_groupidx\
                                    ,src_weight_wordidx=self.src_weight_wordidx\
                                    ,is_debug_mode=self.is_debug_mode)
            # print('rt base score: {}'.format(rt.base_score_weighted))
            if self.will_dump_object and len(self.trg_rownum_todebug_list) > 0:
                    x = Dump(self.dump_directory,rt.base_word_matched_lod,"baselod({},{})".format(rt.rowid,rt.rowid))
                    x.tocsv()
            return rt



class Dump:
    """plain data dump"""
    def __init__(self,dirname,data, name=None):
        self.dirname = dirname
        self.data = data
        self.name = name

    def totext(self):
        timestamp = str(datetime.now())
        timestamp = re.sub("[-:.]","",timestamp)
        if self.name is not None:
            filename = "dump_" + self.name
        else:
            filename = "dump"
        path = os.path.join(self.dirname,filename + timestamp + ".txt")
        try:
            file = open(path,"w")
            if type(self.data) is list or type(self.data) is tuple:
                for row in self.data:
                    try:
                        file.write(str(row)+ '\n') #
                    except Exception as e:
                        print('warning! {} with reference to writing row {}.'.format(e,row))
            else:
                file.write(str(self.data))
        except Exception as e:
            print('warning! {} with reference to dumping object.'.format(e))
            pass

    def tocsv(self):
        timestamp = str(datetime.now())
        timestamp = re.sub("[-:.]","",timestamp)
        if self.name is not None:
            filename = "dump_" + self.name
        else:
            filename = "dump"
        path = os.path.join(self.dirname,filename + timestamp + ".csv")
        try:
            file = open(path,"w")
            if type(self.data) is list or type(self.data) is tuple:
                i = 0
                for row in self.data:
                    columnname_list = list(row.keys())
                    data_list = list(row.values())
                    try:
                        if i == 0: #columns and data
                            file.write(','.join(columnname_list) + '\n')
                            file.write(','.join(str(x) for x in data_list) + '\n')
                        if i > 0: #data only
                            file.write(','.join(str(x) for x in data_list) + '\n')
                    except Exception as e:
                        print('warning! {} with reference to writing row {}.'.format(e,row))

                    i += 1
            else:
                file.write(str(self.data))
        except Exception as e:
            print('warning! {} with reference to dumping object.'.format(e))
            pass


#################
# helper/non core classes
#################
class ProgressBar:
    def __init__(self,total_tasks):
        self.total_tasks = total_tasks
        self.done_tasks = 0
        self.todo_tasks = 0
        self.progress_pct = 0
        self.progress_bar = ""
        self.todo_char = "."
        self.done_char = ">"
        self.bar_length = 20
        self.todo_len = ""
        self.done_len = ""
        self.bar = ""
        

    def calc_progress(self):
        self.progress_pct = int(self.done_tasks / self.total_tasks * 100)
        self.todo_tasks = self.total_tasks - self.done_tasks
    
    def calc_bar(self):
        self.done_len = int(self.bar_length / self.total_tasks * self.done_tasks )
        self.todo_len = self.bar_length - self.done_len
        # set the bar with the concate 
        self.bar = (self.done_char * self.done_len) + (self.todo_char * self.todo_len)
    


    def set_progress_bar(self):
        self.progress_bar = '[{}] {}/{} [{}%]'.format(self.bar,self.done_tasks,self.total_tasks,self.progress_pct)

    def get_progress(self):
        self.calc_bar()
        self.calc_progress()
        self.set_progress_bar()

        return self.progress_bar