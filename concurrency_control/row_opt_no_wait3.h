/*
   Copyright 2016 Massachusetts Institute of Technology

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

#ifndef ROW_OPT_NO_WAIT_H
#define ROW_OPT_NO_WAIT_H

class Row_opt_no_wait {
public:

	void init(row_t * row);
    RC lock_get(access_t type, itemid_t *m_item,TxnManager * txn);
    RC lock_release(TxnManager * txn,uint64_t rid);

private:
	row_t * _row;
};

#endif
