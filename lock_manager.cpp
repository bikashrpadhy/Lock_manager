/* MTCS-203(DBMS) Assignment A2 -- Lock Manager
 * 
 * Goal : Build a lock manager. 
 * The lock manager should support the following capabilities:
 * 1. Lock a resource in either shared or exclusive mode. 
 * 2. Unlock a resource held by a transaction. 
 * A resource will be identified by a 10 character string.  
 *
 * Submitted by- Bikash Ranjan Padhy,
 * Regd_no: 22554
 */

#include <iostream>
#include <unordered_map>
#include <list>
#include <iterator>
#include<string>

using namespace std;


// Abstraction of a resource that can be locked. 
// A resource is locked in a 'mode' by a 'transaction'. 
// The lock request may be granted or put on wait;
// based on a lock compatibility matrix. 
class lockable_resource
{
private:
  uint32_t txn_id_; // txn_id will be treated as process_id too
  uint8_t lock_type_; // SHARED, EXCLUSIVE
  uint8_t lock_status_; // GRANTED, WAITING
public:
  lockable_resource(uint32_t txn_id, uint8_t lock_type, uint8_t lock_status):
    txn_id_(txn_id),
    lock_type_(lock_type),
    lock_status_(lock_status)
  {}
  uint8_t getLockType() 
  {
    return(lock_type_);
  }
  uint8_t getStatus()
  {
    return(lock_status_);
  }
  uint32_t getTxnId()
  {
    return(txn_id_);
  }
  bool setLockStatus(uint8_t st)
  {
    lock_status_ = st;
    return true;
  }
  string enum_to_string(const int eenum)
  {
  switch (eenum)
  {
    // SHARED=1, EXCLUSIVE=2
    // WAITING=3,GRANTED=4
    case 1:
        return "SHARED";
        break;
    case 2:
        return "EXCLUSIVE";
        break;
    case 3:
        return "WAITING";
        break;
    case 4:
        return "GRANTED";
        break;
    default:
        return "";
        break;
    }
  }
  void displayLockableResource(std::string resource_name)
  {
    // printf("lock_type: %d\n", this->getLockType());
    // printf("lock_status: %d\n", this->getStatus());
    cout<< "\nResource_name: " << resource_name << endl;
    cout<< "txn_id: " << this->getTxnId() << endl;
    cout<< "lock_type: " << enum_to_string(this->getLockType()) << endl;
    cout<< "lock_status: " << enum_to_string(this->getStatus()) << endl;
    cout<<"************************************"<<endl;
  }

};
// end of class lockable_resource

enum lockType
{
  SHARED=1, EXCLUSIVE=2
};

enum lockStatus
{
  WAITING=3,GRANTED=4
  
};

bool lock(std::string resource_name, std::uint32_t txn_id, uint8_t lock_type);

uint8_t unlock(std::string resource_name, std::uint32_t txn_id);

unordered_map<std::string, list<lockable_resource *>*> lock_table;

void displaylock_table(unordered_map<std::string, list<lockable_resource *>*> const &lock_table)
{
  cout<<"\n##########################Printing the LockTable: "<<endl;
  for (auto const &pair: lock_table) 
  {
    cout << "\n$$$$$$$$$$$$$$$ Resource_name: " << pair.first <<""<<endl; //<< ": " << pair.second << "}\n";
    cout<< "Locks and Requests on this Resource are: " <<endl;
    list<lockable_resource*> *lst = pair.second;
    for(auto lr = lst->begin(); lr !=lst->end(); ++lr)
    {
      (*lr)->displayLockableResource(pair.first);
    }
  }
}

int main()
{
  uint8_t ret = lock("AAA", 1234, lockType::SHARED);
  ret = lock("AAA", 4567, lockType::EXCLUSIVE); 
  ret = lock("AAA", 4567, lockType::SHARED); 
  ret= lock("AAA", 4568, lockType::EXCLUSIVE);
  ret= lock("AAA", 4569, lockType::EXCLUSIVE);
  ret= lock("BBB", 4570, lockType::SHARED);
  ret= lock("AAA", 4571, lockType::SHARED);
  ret= lock("BBB", 4572, lockType::EXCLUSIVE);
  cout<<"\n\n  *******************************All Locks acquired"<<endl;
  cout<<"\n\n  *******************************Processing Unlock requests\n"<<endl;
  
  try
  {
    unlock("AAA", 4567);
  }
  catch(std::invalid_argument& e)
  {
    cerr << e.what() << endl;
  }

  displaylock_table(lock_table);
  return 0;

}


/// @brief lock the resource_name with the transaction id as txn_id, else put the lock request in the list
/// @param resource_name the resource on which transaction wants a lock on
/// @param txn_id transaction id of the transaction
/// @param lock_type the kind of lock(Shared/ Exclusive) being requested 
/// @return True if lock was acquired 
bool lock(std::string resource_name, std::uint32_t txn_id, uint8_t lock_type)
{
  uint8_t retval = lockStatus::WAITING;

  // Check if lock exists. 
  //   No: Add to map, create new list and add lockable_resource to list
  if (lock_table.find(resource_name) == lock_table.end())
  {
    // lock table does not exist. 
    //  => lockable resource has to be created. 
    //     list of lockable resources has to be created. 
    //     lock table should be updated with resource. 
    lockable_resource *lr = new lockable_resource(txn_id, lock_type, lockStatus::GRANTED );
    retval = lockStatus::GRANTED;
    list<lockable_resource*> *lst = new list<lockable_resource*>;
    lst->emplace_back(lr);
    lock_table[resource_name] = lst;
    (*lr).displayLockableResource(resource_name); //de-reference the iterator
  }
  else //lock has been granted to resource_name already
  {
    //extract lockable_resource entry
    list<lockable_resource*> *lst = lock_table[resource_name];
    
    std::list<lockable_resource*>::iterator lr;
    lr= lst->begin();
      if((*lr)->getStatus()== lockStatus:: WAITING)  
      {
        // There's a transaction waiting to lock this resource_name
        // Requesting transaction must wait
        lockable_resource *lr = new lockable_resource(txn_id, lock_type, lockStatus::WAITING );
        retval = lockStatus::WAITING;
        lst->emplace_back(lr);
        (*lr).displayLockableResource(resource_name);
      }

      if((*lr)->getLockType()== lockType:: EXCLUSIVE && (*lr)->getStatus()== lockStatus:: GRANTED)
      { 
        //resource_name is locked in Exclusive mode by another transaction
        //Requesting transaction must wait
        lockable_resource *lr = new lockable_resource(txn_id, lock_type, lockStatus::WAITING );
        retval = lockStatus::WAITING;
        lst->emplace_back(lr);
        (*lr).displayLockableResource(resource_name);
      }

      else if((*lr)->getLockType()== lockType:: SHARED && (*lr)->getStatus()== lockStatus:: GRANTED) 
      {
        //Resource is locked in Shared mode by another transaction
        //Requesting transaction should wait if it wants an Exclusive lock, else shared lock can be granted.
        if(lock_type== lockType:: SHARED)
        {
          lockable_resource *lr = new lockable_resource(txn_id, lock_type, lockStatus::GRANTED );
          retval = lockStatus::GRANTED;
          lst->emplace_back(lr);
          (*lr).displayLockableResource(resource_name);
        }
        else//lock_type is EXCLUSIVE
        {
          lockable_resource *lr = new lockable_resource(txn_id, lock_type, lockStatus::WAITING );
          retval = lockStatus::WAITING;
          lst->emplace_back(lr);
          (*lr).displayLockableResource(resource_name);
        }
      }
      
  }

  return(retval);

}


/// @brief unlock the resource_name with the transaction id as txn_id, else put the unlock request in the list
/// @param resource_name the resource on which transaction wants a lock on
/// @param txn_id transaction id of the transaction
/// @return lockStatus::  GRANTED/WAITING 
uint8_t unlock(std::string resource_name, std::uint32_t txn_id)
{
  uint8_t retval = lockStatus::WAITING;
  if (lock_table.find(resource_name) == lock_table.end())
  {
    // Couldnt find the resource to be unlocked in the lock_table
    // Invalid unlock request, throw an exception (error message)
    throw std::invalid_argument("Cant unlock something that's not locked");
  }
  else
  {
    // found an entry in the lock_table 
    // check the transaction_id(lr.txn_id== txn_id) and then check if the lock is already GRANTED. 
    // only then the unlock request is valid.
    // if the unlock request in valid;
    // if valid,   unlock the resource and promote the waiting transactions waiting after this transaction in the queue(list)
    // Remove the lr entry from the lst
    // return (unlock) GRANTED
    // else return (unlock) WAITING 
    list<lockable_resource*> *lst = lock_table[resource_name];
    bool not_granted;

    auto lr = lst->begin();
    for( ; lr !=lst->end(); ++lr)
    {
      if((*lr)->getTxnId()== txn_id ) 
      {
        if((*lr)->getStatus()== lockStatus::GRANTED)
        {
          cout<<"\n$$$$$$$$$$$$   Found this transaction's lockable resource to be unlocked: "<<endl;
          (*lr)->displayLockableResource(resource_name);
          std::list<lockable_resource*>::iterator templr;
          templr= lr;
          // lr now points to the first valid transaction
          ++lr;
          // lr now points to the next transaction in the queue
          lst->erase(templr);
          not_granted= false;
          break;
        }
        else
        {
          // Found the lockable resource of this txn_id, but its still in WAITING lockStatus
          not_granted= true;
        }
      }
      
      // Found the correct txn_id, removed it from the queue. 
    }
    if(not_granted)
    {
      cout<<"Lock wasn't GRANTED to txn_id: "<< txn_id <<". So it must wait for "<< resource_name<< " to be unlocked by previous transactions."<<endl;
      return retval;
    }

    bool shared_flag=false;
    for(lr=lst->begin() ; lr!= lst->end(); ++lr)
    {
      // promote the waiting transaction(s) waiting in the queue(list)
      // yeah, especaially the ones that came before the unlocked transaction
      if((*lr)->getStatus()== lockStatus::WAITING)
      {
        if((*lr)->getLockType()== lockType::SHARED)
        {
          //promote all SHARED
          (*lr)->setLockStatus(lockStatus::GRANTED);
          retval= lockStatus::GRANTED; //unlock has been granted
          shared_flag= true;        
          continue;
        }
        else if( shared_flag!=true && (*lr)->getLockType()== lockType::EXCLUSIVE )
        {
          //promote only one EXCLUSIVE
          (*lr)->setLockStatus(lockStatus::GRANTED);
          retval= lockStatus::GRANTED; //unlock has been granted
          break;        
        }
      }      
    }
  }
  return retval;

}
