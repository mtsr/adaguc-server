/******************************************************************************
 * 
 * Project:  ADAGUC Server
 * Purpose:  MongoDB driver ADAGUC Server
 * Author:   Rob Tjalma, tjalma "at" knmi.nl
 * Date:     2015-09-18
 *
 ******************************************************************************
 *
 * Copyright 2013, Royal Netherlands Meteorological Institute (KNMI)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 ******************************************************************************/
#ifdef ADAGUC_USE_KDCMONGODB
#include "mongo/client/dbclient.h"
#include "mongo/bson/bson.h"
#include <boost/algorithm/string.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/xml_parser.hpp>
#include "CDBAdapterMongoDB.h"
#include <set>
#include "CDebugger.h"
#include "CGetFileInfo.h"
#include <fstream>
#include <limits>
#include <string>
#include <sstream>
#include <algorithm>

/* ---- CONSTANTS AND GLOBAL VARIABLES ---- */
/* ---------------------------------------- */
const char *CDBAdapterMongoDB::className="CDBAdapterMongoDB";

//#define CDBAdapterMongoDB_DEBUG

/* Default values for mongo queries */
#define N_TO_RETURN_0 0
#define N_TO_RETURN_1 1
#define N_TO_SKIP_0 0

/* The configuration XML file. */
CServerConfig::XMLE_Configuration *configurationObject;

/* Is the dataset config written or not? Only needed once! */
bool configWritten = false;

/* Used to discover the sorting of the MongoDB queries. */
std::map<std::string,std::string> tableCombi;

/* To remember the current used dimension, define a global variable. */
const char* currentUsedDimension = "";

/* The current used dataset name and version. */
const char* dataSetName = "-";
const char* dataSetVersion = "-";

/* Constants for defining the tables for the granules
 * and datasets in MongoDB. */
const char* dataSetsTableMongoDB = "database.dataSets";
const char* dataGranulesTableMongoDB = "database.datagranules";

/* Current used layer. */
CServerConfig::XMLE_Layer * cfgLayer;

/* */
CT::string conditionalSecondDimension = "";
/* ---------------------------------------- */

DEF_ERRORMAIN();

/*
 * Getting the database connection. 
 * This includes connecting and authenticating, 
 * with the params from the config file.
 * 
 * @return the DBClientConnection object, containing the database connection. 
 *         If no connection can be established, return NULL.
 */
mongo::DBClientConnection *dataBaseConnection;

mongo::DBClientConnection *getDataBaseConnection() {
    
    #ifdef MEASURETIME
        StopWatch_Stop(">CDBAdapterMongoDB::getDatabaseConnection");
    #endif
    
    if(dataBaseConnection == NULL) {
        // Variable for holding the error message.
        std::string errorMessage;
    
        // Getting the parameters for connecting to the MongoDB database,
        // like host, port and username.
        CT::string parameters = configurationObject->DataBase[0]->attr.parameters.c_str();
        CT::string *splittedParameters = parameters.splitToArray(" ");
    
        /* Connecting. */
        dataBaseConnection = new mongo::DBClientConnection();
        dataBaseConnection->connect(splittedParameters[0].c_str(),errorMessage);

        /* Authenticating. */
        dataBaseConnection->auth("database", splittedParameters[1].c_str(), splittedParameters[2].c_str(),errorMessage); 

        if(!errorMessage.empty()){
            CDBError("Unable to connect to DB: %s",errorMessage.c_str());
            return NULL;
        }
    }
  

    #ifdef MEASURETIME
        StopWatch_Stop("<CDBAdapterMongoDB::getDatabaseConnection");
    #endif
    
    return dataBaseConnection;
}

/*
 * Writing the dataset config file to the MongoDB database. This way, no extra config file is needed.
 * 
 * @param     CT::string    The path to the XML.
 * @param   CT::string    The dataset path to write.
 * @return    int     Always returns 0.
 */
int CDBAdapterMongoDB::writeConfigXMLToDatabase(CT::string filePath, CT::string datasetPath) {
    
    #ifdef MEASURETIME
        StopWatch_Stop(">CDBAdapterMongoDB::writeConfigXMLToDatabase");
    #endif
  
    mongo::DBClientConnection * DB = getDataBaseConnection();
    if(DB == NULL) {
        return -1;
    }

    /* Retrieve the dataset name and version. */
    
    /* Getting the number of directories of a granule. */
    CT::string VFSPath = configurationObject->VFSPath[0]->attr.value.c_str();
    size_t VfsDirectoriesCount = VFSPath.splitToArray("/")->count;
  
    /* Setting the global variables. */
    dataSetName = datasetPath.splitToArray("/")[VfsDirectoriesCount].c_str();
    dataSetVersion = datasetPath.splitToArray("/")[VfsDirectoriesCount + 1].c_str();
  
    /* Getting the complete XML config. */
    CT::string fileInfo = CGetFileInfo::getLayersForFile(filePath.c_str());
    fileInfo.replaceSelf("[DATASETPATH]",datasetPath.c_str());
  
    /* Complete query for selecting the correct dataset. */
    mongo::BSONObjBuilder queryBuilder;
    queryBuilder << "dataSetName" << dataSetName << "dataSetVersion" << dataSetVersion;
    mongo::BSONObj query = queryBuilder.obj();
  
    /* The data to be stored. */
    mongo::BSONObjBuilder adagucConfigQueryBuilder;
    adagucConfigQueryBuilder << "$set" << BSON("adagucConfig" << fileInfo.c_str());
    mongo::BSONObj adagucConfigQuery = adagucConfigQueryBuilder.obj();
  
    /* Update. */
    DB->update(dataSetsTableMongoDB, mongo::Query(query), adagucConfigQuery);
  
    #ifdef MEASURETIME
        StopWatch_Stop("<CDBAdapterMongoDB::writeConfigXMLToDatabase");
    #endif
        
    return 0;
}

/*
 * Reading the dataset config file from the MongoDB database. 
 * 
 * @param     CT::string    The dataset name to search for.
 * @param   CT::string    The dataset version to search for.
 * @return    CT::string    The complete dataset config as XML.
 */
CT::string CDBAdapterMongoDB::getAdagucConfig(CT::string dataSetName, CT::string dataSetVersion) {
    
    #ifdef MEASURETIME
        StopWatch_Stop(">CDBAdapterMongoDB::getAdagucConfig");
    #endif

    mongo::DBClientConnection * DB = getDataBaseConnection();
    if(DB == NULL) {
        return "";
    }
  
    /* Making the query. */
    mongo::BSONObjBuilder queryBuilder;
    queryBuilder << "dataSetName" << dataSetName.c_str() << "dataSetVersion" << dataSetVersion.c_str();
    mongo::BSONObj query = queryBuilder.obj();
  
    /* Selecting which fields we want to get from the query. */
    mongo::BSONObjBuilder selectQueryBuilder;
    selectQueryBuilder << "adagucConfig" << 1 << "_id" << 0;
    mongo::BSONObj selectQuery = selectQueryBuilder.obj();
  
    /* Executing the query. */
    std::auto_ptr<mongo::DBClientCursor> queryResultCursor = DB->query(dataSetsTableMongoDB, mongo::Query(query), N_TO_RETURN_0, N_TO_SKIP_0, &selectQuery);
  
    CT::string returnValue = "";
    if(queryResultCursor->more()) {
        returnValue = queryResultCursor->next().getStringField("adagucConfig");
    } else {
        CDBError("No adagucConfig field available for dataset %s with version %s", dataSetName.c_str(), dataSetVersion.c_str()); 
    }
  
    return returnValue;
}

/*
 * If the granule WITH adaguc field already in the database?
 * 
 * @param     const char*   The filename of the granule.
 * @param     const char*   Not used.
 * @return    int     0 for no changes, 1 for error.
 */
int checkTableMongo(const char * pszTableName,const char *pszColumns) {
    
    #ifdef MEASURETIME
        StopWatch_Stop(">CDBAdapterMongoDB::checkTableMongo");
    #endif
  
    mongo::DBClientConnection * DB = getDataBaseConnection();
    if(DB == NULL) {
        return 1;
    }
    
    /* Making the query. */
    mongo::BSONObjBuilder queryBuilder;
    queryBuilder << "fileName" << pszTableName << "dataSetName" << dataSetName << "dataSetVersion" << dataSetVersion;
    mongo::BSONObj query = queryBuilder.obj();
  
    /* Selecting which fields we want to get from the query. */
    mongo::BSONObjBuilder selectQueryBuilder;
    selectQueryBuilder << "adaguc" << 1 << "_id" << 0;
    mongo::BSONObj selectQuery = selectQueryBuilder.obj();
  
    /* Executing the query. */
    std::auto_ptr<mongo::DBClientCursor> queryResultCursor = DB->query(dataGranulesTableMongoDB, mongo::Query(query), N_TO_RETURN_0, N_TO_SKIP_0, &selectQuery);
  
    /* The only thing we need is the 'adaguc' field of the MongoDB record. */
    mongo::BSONObj record = queryResultCursor->next().getObjectField("adaguc");
  
    /* There must be returned exactly 1 field ( path ), otherwise they don't exist. */
    if(record.nFields() == 0) {
        
        CDBError("Error: No adaguc field in granule with fileName %s.", pszTableName);
        
        /* Return 2 for bad. */
        return 2;
    }
    
    #ifdef MEASURETIME
        StopWatch_Stop("<CDBAdapterMongoDB::checkTableMongo");
    #endif
  
    /* Return 0 for success. */
    return 0;
}

/*
 * Constructor.
 */
CDBAdapterMongoDB::CDBAdapterMongoDB(){
    
    #ifdef MEASURETIME
        StopWatch_Stop(">CDBAdapterMongoDB::CDBAdapterMongoDB");
    #endif
        
    /* These were the three stores being used in the PostgreSQL driver, so reusing them. */
    tableCombi.insert(std::make_pair("pathfiltertablelookup","path,filter,dimension,tablename"));
    tableCombi.insert(std::make_pair("autoconfigure_dimensions","layer,ncname,ogcname,units"));
    tableCombi.insert(std::make_pair("all_other","path,time,dimtime,filedate"));
    
    dataBaseConnection = NULL;
    
    #ifdef MEASURETIME
        StopWatch_Stop("<CDBAdapterMongoDB::CDBAdapterMongoDB");
    #endif
}

/*
 * Destructor.
 */
CDBAdapterMongoDB::~CDBAdapterMongoDB() {
    
    #ifdef MEASURETIME
        StopWatch_Stop(">CDBAdapterMongoDB::~CDBAdapterMongoDB");
    #endif
        
    mongo::BSONObj info_logging_out;
    
    /* Correctly logging off. */
    if(dataBaseConnection!=NULL){
        dataBaseConnection->logout("database", info_logging_out);
    }
    
    /* And delete it! */
    delete dataBaseConnection;
    
    #ifdef MEASURETIME
        StopWatch_Stop("<CDBAdapterMongoDB::~CDBAdapterMongoDB");
    #endif
}

/*
 * Getting the corrected Column name, based on the MongoDB structure.
 * 
 * @param   const char*   The column name as used in PostgreSQL.
 * @return    const char*   The corrected column name.
 */
const char* CDBAdapterMongoDB::getCorrectedColumnName(const char* column_name) {
        
    std::string prefix = "adaguc.";
        
    return prefix.append(column_name).c_str();
}

/*
 * Check if the ending of a string is as expected.
 * 
 * @param   std::string   The string to search in.
 * @param   std::string   The expected ending of the string.
 * @return    boolean     Was your assumption correct?
 */
bool hasEnding(std::string const &fullString, std::string const &ending) {

    if (fullString.length() >= ending.length()) {
        return (0 == fullString.compare (fullString.length() - ending.length(), ending.length(), ending));
    } else {
        return false;
    }
}

/*
 * Get the current used dimension.
 * If no dimension is known to men, return 
 * default dimension "time".
 * 
 * @return    const char*  The current used dimension.
 */
const char* getCurrentDimension() {
  /* Get value, default is time. */
  if (!conditionalSecondDimension.empty()) {
    return conditionalSecondDimension.c_str();
  } else {
    return "time";
  }
}

/*
 * Check for the nth index of a string.
 * 
 * @param   char*     The string to search in.
 * @param   char*     The string to search for
 * @return    int     The nth occurence.
 */
int strpos(char *hayStack, char *needle, int nth) {
    
    #ifdef MEASURETIME
        StopWatch_Stop(">CDBAdapterMongoDB::strpos");
    #endif
        
    char *res = hayStack;
    for(int i = 1; i <= nth; i++)
    {
        res = strstr(res, needle);
        if (!res) {
            return -1;
        } else if(i != nth) {
            res = res + 1;
        }
    }
    
    #ifdef MEASURETIME
        StopWatch_Stop("<CDBAdapterMongoDB::strpos");
    #endif
        
    return res - hayStack;
}

std::string numberToString(int pNumber)
{
 std::ostringstream oOStrStream;
 oOStrStream << pNumber;
 return oOStrStream.str();
}

/*
 *  Converting the query to a CDBStore, compatible with ADAGUC.
 * 
 *  @param    DBClientCursor    the cursor with a pointer to the query result.
 *  @param    const char*     Used for having the field names in the correct order.
 *  @return   CDBStore::Store   The store containing the results.
 */
CDBStore::Store *ptrToStore(std::auto_ptr<mongo::DBClientCursor> cursor, const char* table, int dimIndex, bool isAggregation = false, int limitInDocuments = 1) {
    
    #ifdef MEASURETIME
        StopWatch_Stop(">CDBAdapterMongoDB::ptrToStore");
    #endif
   
    /* Prework. */
    std::string delimiter = ",";
    /* Fieldsnames cannot be collected from the query unfortunately. So we use static fields. */
    std::string usedColumns = table;
    std::string fileNameOfGranule;
    /* Number of columns that are being used. */
    size_t colNumber = 0;

    /* Variable used for determine if <filename>_<layername> is being used. */
    bool usingExtendedLayerID = false;
  
    /* Get the first one. Get directly the adaguc object. */
    mongo::BSONObj firstValue;
  
    /* If the pointer returns no records, return an empty store. */
    if(cursor->more()) {
        firstValue = cursor->next();
        if(firstValue.isEmpty() || firstValue.getObjectField("adaguc").isEmpty()) {
            #ifdef MEASURETIME
                StopWatch_Stop("<CDBAdapterMongoDB::ptrToStore");
            #endif
            return NULL;
        }
    } else {
        #ifdef MEASURETIME
            StopWatch_Stop("<CDBAdapterMongoDB::ptrToStore");
        #endif
        return NULL;
    }
  
    /* Only applicable if table columns are equal to these. */
    if(strcmp(table,tableCombi.find("autoconfigure_dimensions")->second.c_str()) == 0) {
        usingExtendedLayerID = true;
        /* Instead of tablelayer + layername, for MongoDB we use fileName + _ + layername. */
        fileNameOfGranule = firstValue.getStringField("fileName");
        fileNameOfGranule.append("_");
    }
  
    CT::string gottenTable(table);
    CT::string* columnsAsArray = gottenTable.splitToArray(",");
    /* Number of columns. */
    size_t numCols = columnsAsArray->count;

    CDBStore::ColumnModel *colModel = new CDBStore::ColumnModel(numCols);
  
    /* Making a copy of the used columns. */
    usedColumns = table;
  
    /* Filling all column names. */
    for(size_t j = 0; j < numCols; j++) {
        std::string cName = usedColumns.substr(0,usedColumns.find(delimiter));
        usedColumns.erase(0,usedColumns.find(delimiter) + 1);
        /* If true, it means the first column is layer. Make it layerid, so adaguc knows the column. */
        if(j == 0 && true == usingExtendedLayerID) { cName.append("id"); }
        colModel->setColumn(colNumber,cName.c_str());
        colNumber++;
    }
  
    /* Creating a store */
    CDBStore::Store *store=new CDBStore::Store(colModel);

    /* Using a vector for future purposes ( aggregations ). */
    std::vector<mongo::BSONElement> VectorWithDimensionValues;
    std::vector<mongo::BSONElement>::iterator it;
    /* Reset the colNumber. */
    colNumber = 0;

    /* Getting the name of a 'step in the dimension'. */
    std::string dimOfDimension = "dim";
    dimOfDimension.append(getCurrentDimension());
    const char* dimDimension = dimOfDimension.c_str();
    
    int numberOfTimes = 1;
    int indexOfDimension = 0;
    
    if(strcmp(table,"time,") == 0) {
        numberOfTimes = firstValue.getObjectField("adaguc").getField(getCurrentDimension()).Array().size();
    }
    if(strcmp(table,"path,time,dimtime") == 0 && limitInDocuments != 1 && isAggregation) {
        indexOfDimension = dimIndex;
        numberOfTimes = limitInDocuments + indexOfDimension;
    }
    
    while(indexOfDimension < numberOfTimes) {
        usedColumns = table;
        
        colNumber = 0;

        CDBStore::Record *record = new CDBStore::Record(colModel);
        
        for(size_t j = 0; j < numCols; j++) {
            std::string cName = usedColumns.substr(0,usedColumns.find(delimiter));
            usedColumns.erase(0,usedColumns.find(delimiter) + 1);
            std::string fieldValue;
            // If time or dimtime is being used:
            if(strcmp(cName.c_str(),getCurrentDimension()) == 0 || strcmp(cName.c_str(),"time2D") == 0) {
                VectorWithDimensionValues = firstValue.getObjectField("adaguc").getField(cName.c_str()).Array();
                if(numberOfTimes != 1 && isAggregation ) {
                    fieldValue = VectorWithDimensionValues[indexOfDimension].str();
                } else if ( isAggregation ) {
                    fieldValue = VectorWithDimensionValues[dimIndex].str();
                } else {
                    fieldValue = VectorWithDimensionValues[0].str();
                }
            } else if(strcmp(cName.c_str(),dimDimension) == 0) {
                if(numberOfTimes != 1 && isAggregation) {
                    fieldValue = numberToString(indexOfDimension);
                } else if ( isAggregation ) {
                    fieldValue = numberToString(dimIndex);
                } else {
                    fieldValue = "0";
                }
            } else if(strcmp(cName.c_str(), "ncname") == 0) {
                fieldValue = getCurrentDimension();
            } else if(strcmp(cName.c_str(), "filedate") == 0) {
                fieldValue = firstValue.getObjectField("adaguc").getStringField("filedate");
            } else if(strcmp(cName.c_str(), "ogcname") == 0) {
                const char* the_dimension = cfgLayer->Name[0]->value.c_str();
                fieldValue = firstValue.getObjectField("adaguc").getObjectField("layer").getObjectField(the_dimension).getObjectField("dimension").getObjectField(getCurrentDimension()).getStringField("ogcname");
            } else if(strcmp(cName.c_str(), "layer") == 0) {
                fieldValue = cfgLayer->Name[0]->value.c_str();
            } else {
                fieldValue = firstValue.getObjectField("adaguc").getStringField(cName.c_str());
            }
            
            if(j == 0 && true == usingExtendedLayerID) {
                fieldValue = fileNameOfGranule.append(fieldValue); 
            }
            
            record->push(colNumber,fieldValue.c_str());
            colNumber++;
        } 
    
        /* And push it! */
        store->push(record);
        indexOfDimension = indexOfDimension + 1;
    }
  
    /* Then the next values. */
    while(cursor->more()) {
        mongo::BSONObj nextValue = cursor->next();
        
        if(strcmp(table,"path,time,dimtime") == 0 && limitInDocuments != 1 && isAggregation) {
            indexOfDimension = dimIndex;
        } else {
            indexOfDimension = 0;
        }
        
        while(indexOfDimension < numberOfTimes) {
            usedColumns = table;
            colNumber = 0;
            CDBStore::Record *record = new CDBStore::Record(colModel);
            for(size_t j = 0; j < numCols; j++) {
                std::string cName = usedColumns.substr(0,usedColumns.find(delimiter));
                usedColumns.erase(0,usedColumns.find(delimiter) + 1);
                std::string fieldValue;
                // If time or dimtime is being used:
                if(strcmp(cName.c_str(),getCurrentDimension()) == 0 || strcmp(cName.c_str(),"time2D") == 0) {
                    VectorWithDimensionValues = nextValue.getObjectField("adaguc").getField(cName.c_str()).Array();
                    if(numberOfTimes != 1 && isAggregation) {
                        fieldValue = VectorWithDimensionValues[indexOfDimension].str();
                    } else if ( isAggregation ) {
                        fieldValue = VectorWithDimensionValues[dimIndex].str();
                    } else {
                        fieldValue = VectorWithDimensionValues[0].str();
                    }
                } else if(strcmp(cName.c_str(),dimDimension) == 0) {
                    if(numberOfTimes != 1 && isAggregation) {
                        fieldValue = numberToString(indexOfDimension);
                    } else if ( isAggregation ) {
                        fieldValue = numberToString(dimIndex);
                    } else {
                        fieldValue = "0";
                    }
                } else if(strcmp(cName.c_str(), "ncname") == 0) {
                    fieldValue = getCurrentDimension();
                } else if(strcmp(cName.c_str(), "filedate") == 0) {
                    fieldValue = nextValue.getObjectField("adaguc").getStringField("filedate");
                } else if(strcmp(cName.c_str(), "ogcname") == 0) {
                    const char* the_dimension = cfgLayer->Name[0]->value.c_str();
                    fieldValue = nextValue.getObjectField("adaguc").getObjectField("layer").getObjectField(the_dimension).getObjectField("dimension").getObjectField(getCurrentDimension()).getStringField("ogcname");
                } else if(strcmp(cName.c_str(), "layer") == 0) {
                    fieldValue = cfgLayer->Name[0]->value.c_str();
                } else {
                    fieldValue = nextValue.getObjectField("adaguc").getStringField(cName.c_str());
                }
                
                if(j == 0 && true == usingExtendedLayerID) {
                    fieldValue = fileNameOfGranule.append(fieldValue); 
                }
                record->push(colNumber,fieldValue.c_str());
                colNumber++;
            } 
            
            store->push(record);
            indexOfDimension = indexOfDimension + 1;
        }
    }
    
    #ifdef MEASURETIME
        StopWatch_Stop("<CDBAdapterMongoDB::ptrToStore");
    #endif
  
    return store;
}

/*
 * Setting the config file.
 * 
 * @param   XMLE_Configuration  The config file.
 * @return    int     Returns 0. 
 */
int CDBAdapterMongoDB::setConfig(CServerConfig::XMLE_Configuration *cfg) {
        
    configurationObject = cfg;

    return 0;
}

/*
 * Not made yet!
 */
CDBStore::Store *CDBAdapterMongoDB::getReferenceTime(const char *netcdfDimName,const char *netcdfTimeDimName,const char *timeValue,const char *timeTableName,const char *tableName) {
    
    return NULL;
}

/*
 * Not made yet!
 */
CDBStore::Store *CDBAdapterMongoDB::getClosestDataTimeToSystemTime(const char *netcdfDimName,const char *tableName) {
    
    mongo::DBClientConnection * DB = getDataBaseConnection();
    
    if(DB == NULL) {
        return NULL;
    }
  
    /*
    mongo::BSONObjBuilder querySelect;
    querySelect << netcdfDimName << 1;
    mongo::BSONObj objBSON = querySelect.obj();
    
    std::auto_ptr<mongo::DBClientCursor> cursorFromMongoDB;
    cursorFromMongoDB = DB->query(tableName,mongo::Query(objBSON).sort("",1), 1, 0);
    
    DATENOW
    //query.print("SELECT %s,abs(EXTRACT(EPOCH FROM (%s - now()))) as t from %s order by t asc limit 1",netcdfDimName,netcdfDimName,tableName);
    return ptrToStore(cursorFromMongoDB, tableName.c_str());
    */
  
    return NULL;
}

/*
 * Retrieving the filename of the granule or dataset.
 * 
 * @param     const char*   The full path of the granule/dataset.
 * @param   const char*   Not used.
 * @param   const char*   Not used.
 * @param   CDataSource   A data source containing all information of the chosen granule/dataset.
 * @return    CT::string    Name of the granule/dataset.
 */
CT::string CDBAdapterMongoDB::getTableNameForPathFilterAndDimension(const char *path,const char *filter, const char * dimension,CDataSource *dataSource) {
    
    #ifdef MEASURETIME
        StopWatch_Stop(">CDBAdapterMongoDB::getTableNameForPathFilterAndDimension");
    #endif

    /*
     * If there is a secondary dimension, get it here.
     */

    if(dimension != NULL && strcmp(currentUsedDimension,dimension) != 0 && 
        strcmp(currentUsedDimension,"") != 0) {
        conditionalSecondDimension = "";
        conditionalSecondDimension.concat(dimension);
    }
  
    /* Setting the current layer as a global variable. Needs to be used in the ptrToStore.
     * Chosen to be a global variable, may be improved in the future. 
     */
    cfgLayer = dataSource->cfgLayer;

    /* Making a string, so method of the CT::string class can be used. */
    CT::string thePath = path;
    /* Get the 6th part of the splitted string. This is always the dataset name. */
    dataSetName = thePath.splitToArray("/")[5].c_str();
    dataSetVersion = thePath.splitToArray("/")[6].c_str();

    /* Formatting the path, so the last part is only used. */
    if(!hasEnding(path, "/")) {
        /* If it doesn't end with the '/' character, it is a granule. */
        thePath.substringSelf(&thePath,(size_t)thePath.lastIndexOf("/") + 1,thePath.length());
    }
    
    #ifdef MEASURETIME
        StopWatch_Stop("<CDBAdapterMongoDB::getTableNameForPathFilterAndDimension");
    #endif
  
    /* The path is the fileName of the granule or dataSetName of a dataset in the MongoDB database. */
    return thePath;
}

/*
 * Not totally finished. Completing when writing to DB is done.
 */
int CDBAdapterMongoDB::autoUpdateAndScanDimensionTables(CDataSource *dataSource) {
    
    #ifdef MEASURETIME
        StopWatch_Stop(">CDBAdapterMongoDB::autoUpdateAndScanDimensionTables");
    #endif
        
    mongo::DBClientConnection * DB = getDataBaseConnection();
    if(DB == NULL) {
        return -1;
    }
        
    CServerParams *srvParams = dataSource->srvParams;
    CServerConfig::XMLE_Layer * cfgLayer = dataSource->cfgLayer;
  
    CCache::Lock lock;
    CT::string identifier = "checkDimTables";  identifier.concat(cfgLayer->FilePath[0]->value.c_str());  identifier.concat("/");  identifier.concat(cfgLayer->FilePath[0]->attr.filter.c_str());  
    CT::string cacheDirectory = srvParams->cfg->TempDir[0]->attr.value.c_str();
    //srvParams->getCacheDirectory(&cacheDirectory);
    if(cacheDirectory.length() > 0){
        lock.claim(cacheDirectory.c_str(),identifier.c_str(),"checkDimTables",srvParams->isAutoResourceEnabled());
    }
  
    #ifdef CDBAdapterMongoDB_DEBUG
        CDBDebug("[checkDimTables]");
    #endif
        
    bool tableNotFound=false;
    bool fileNeedsUpdate = false;
    CT::string dimName;
    for(size_t i=0;i<cfgLayer->Dimension.size();i++){
        dimName=cfgLayer->Dimension[i]->attr.name.c_str();
    
        CT::string tableName;
        try{
            tableName = getTableNameForPathFilterAndDimension(cfgLayer->FilePath[0]->value.c_str(),cfgLayer->FilePath[0]->attr.filter.c_str(), dimName.c_str(),dataSource);
        }catch(int e){
            CDBError("Unable to create tableName from '%s' '%s' '%s'",cfgLayer->FilePath[0]->value.c_str(),cfgLayer->FilePath[0]->attr.filter.c_str(), dimName.c_str());
            return 1;
        }
    
        CT::string dimNameAdaguc = "adaguc.";
        dimNameAdaguc.concat(dimName.c_str());
        currentUsedDimension = dimName.c_str();
        
        mongo::BSONObjBuilder queryForSelecting;
        queryForSelecting << "adaguc.path" << 1 << "adaguc.filedate" << 1 << dimNameAdaguc.c_str() << 1 << "_id" << 0;
        mongo::BSONObj objBSON = queryForSelecting.obj();
        
        mongo::BSONObjBuilder query_builder;
        if(!hasEnding(tableName.c_str(), "/")) {
            query_builder << "fileName" << tableName.c_str() << "dataSetName" << dataSetName << "dataSetVersion" << dataSetVersion;
        } else {
            query_builder << "adaguc.dataSetPath" << tableName.c_str() << "dataSetName" << dataSetName << "dataSetVersion" << dataSetVersion; 
        }
        
        mongo::BSONObj the_query = query_builder.obj();
        
        std::auto_ptr<mongo::DBClientCursor> queryResultCursor;
        
        queryResultCursor = DB->query(dataGranulesTableMongoDB,mongo::Query(the_query),N_TO_RETURN_1, N_TO_SKIP_0, &objBSON);

        CT::string columnToReturn = "path,filedate,";
        columnToReturn.concat(dimName.c_str());
        
        CDBStore::Store *store = ptrToStore(queryResultCursor, columnToReturn.c_str(), 0);
        
        if(store==NULL){
            tableNotFound=true;
            CDBDebug("No table found for dimension %s",dimNameAdaguc.c_str());
        }
        
        if(tableNotFound == false){
            if(srvParams->isAutoLocalFileResourceEnabled()==true){
                try{
                    CT::string databaseTime = store->getRecord(0)->get(1);if(databaseTime.length()<20){databaseTime.concat("Z");}databaseTime.setChar(10,'T');
            
                    CT::string fileDate = CDirReader::getFileDate(store->getRecord(0)->get(0)->c_str());
            
                if(databaseTime.equals(fileDate)==false){
                    CDBDebug("Table was found, %s ~ %s : %d",fileDate.c_str(),databaseTime.c_str(),databaseTime.equals(fileDate));
                    fileNeedsUpdate = false;
                }
            
                }catch(int e){
                    CDBDebug("Unable to get filedate from database, error: %s",CDBStore::getErrorMessage(e));
                    fileNeedsUpdate = true;
                }
            }
        }
        
        delete store;
        if(tableNotFound||fileNeedsUpdate)break;
    }
  
    if(fileNeedsUpdate == true){
        //Recreate table
        if(srvParams->isAutoLocalFileResourceEnabled()==true){
            for(size_t i=0;i<cfgLayer->Dimension.size();i++){
                dimName=cfgLayer->Dimension[i]->attr.name.c_str();
      
                CT::string tableName;
                try{
                    tableName = getTableNameForPathFilterAndDimension(cfgLayer->FilePath[0]->value.c_str(),cfgLayer->FilePath[0]->attr.filter.c_str(), dimName.c_str(),dataSource);
                }catch(int e){
                    CDBError("Unable to create tableName from '%s' '%s' '%s'",cfgLayer->FilePath[0]->value.c_str(),cfgLayer->FilePath[0]->attr.filter.c_str(), dimName.c_str());
                    return 1;
                }
                
                CDBFileScanner::markTableDirty(&tableName);
                //CDBDebug("Dropping old table (if exists)",tableName.c_str());
                CT::string query ;
                query.print("drop table %s",tableName.c_str());
                CDBDebug("Try to %s for %s",query.c_str(),dimName.c_str());
                dataBaseConnection->query(query.c_str());
            }
        tableNotFound = true;
        }
    }
 
    if(tableNotFound){
        if(srvParams->isAutoLocalFileResourceEnabled()==true){
            CDBDebug("Updating database");
            int status = CDBFileScanner::updatedb(dataSource,NULL,NULL,0);
            if(status !=0){CDBError("Could not update db for: %s", cfgLayer->Name[0]->value.c_str());return 2;}
        }else{
            CDBDebug("No table found for dimension %s and autoresource is disabled",dimName.c_str());
            return 1;
        }
    }
    
    #ifdef CDBAdapterMongoDB_DEBUG
        CDBDebug("[/checkDimTables]");
    #endif
    lock.release();
    #ifdef MEASURETIME
        StopWatch_Stop("<CDBAdapterMongoDB::autoUpdateAndScanDimensionTables");
    #endif
    return 0;
}

/*
 * Getting the minimum value of multiple records.
 * 
 * @param     const char*   The current used dimension.
 * @param   const char*   The fileName or dataSetPath.
 * @return    CDBStore::Store   A store containing the minimum value.
 */
CDBStore::Store *CDBAdapterMongoDB::getMin(const char *name,const char *table) {
    
    #ifdef MEASURETIME
        StopWatch_Stop(">CDBAdapterMongoDB::getMin");
    #endif
        
    mongo::DBClientConnection * DB = getDataBaseConnection();
    if(DB == NULL) {
        return NULL;
    }

    /* Get the correct MongoDB path of the current used dimension. */
    std::string usedName = getCorrectedColumnName(name);
  
    /* Selecting query. */
    mongo::BSONObjBuilder queryBuilder;
    if(!hasEnding(table, "/")) {
        queryBuilder << "fileName" << table << "dataSetName" << dataSetName << "dataSetVersion" << dataSetVersion;
    } else {
        queryBuilder << "adaguc.dataSetPath" << table << "dataSetName" << dataSetName << "dataSetVersion" << dataSetVersion; 
    }
    mongo::BSONObj queryObject = queryBuilder.obj();
  
    /* Executing the query and sort by name, in ascending order. */
    mongo::Query query = mongo::Query(queryObject).sort(usedName,1);
    
    // -------------------------------
    
    std::auto_ptr<mongo::DBClientCursor> checkForAggregation = DB->query(dataGranulesTableMongoDB, query , N_TO_RETURN_1, N_TO_SKIP_0);
    
    bool isAggregation = checkForAggregation->next().getObjectField("adaguc").getField(getCurrentDimension()).Array().size() > 1;
    
    // -------------------------------
  
    mongo::BSONObjBuilder queryForSelecting;
    if (isAggregation) {
        queryForSelecting << usedName.c_str() << BSON("$slice" << 1) << "_id" << 0;
    } else {
        queryForSelecting << usedName.c_str() << 1 << "_id" << 0;
    }
    mongo::BSONObj objBSON = queryForSelecting.obj();
  
    /* MongoDB uses std::auto_ptr for getting all records. */
    std::auto_ptr<mongo::DBClientCursor> queryResultCursor;
    queryResultCursor = DB->query(dataGranulesTableMongoDB,query, N_TO_RETURN_1, N_TO_SKIP_0, &objBSON);
  
    /* Only need one record, so only the 'name' variable. */
    std::string buff = name;
    buff.append(",");
    CDBStore::Store *minStore = ptrToStore(queryResultCursor, buff.c_str(), 0);
  
    if(minStore == NULL){
        setExceptionType(InvalidDimensionValue);
        CDBError("Invalid dimension value for  %s",name);
        CDBError("query failed"); 
        return NULL;
    }
    
    #ifdef MEASURETIME
        StopWatch_Stop("<CDBAdapterMongoDB::getMin");
    #endif
        
    return minStore; 
}

/*
 * Getting the maximum value of multiple records.
 * 
 * @param     const char*   The current used dimension.
 * @param   const char*   The fileName or dataSetPath.
 * @return    CDBStore::Store   A store containing the maximum value.
 */
CDBStore::Store *CDBAdapterMongoDB::getMax(const char *name,const char *table) {
    
    #ifdef MEASURETIME
        StopWatch_Stop(">CDBAdapterMongoDB::getMax");
    #endif

    mongo::DBClientConnection * DB = getDataBaseConnection();
    if(DB == NULL) {
        return NULL;
    }
    
    /* Get the correct MongoDB path. */
    std::string usedName = getCorrectedColumnName(name);
  
    mongo::BSONObjBuilder queryBuilder;
    if(!hasEnding(table, "/")) {
        queryBuilder << "fileName" << table << "dataSetName" << dataSetName << "dataSetVersion" << dataSetVersion;
    } else {
        queryBuilder << "adaguc.dataSetPath" << table << "dataSetName" << dataSetName << "dataSetVersion" << dataSetVersion; 
    }
    mongo::BSONObj queryObject = queryBuilder.obj();
  
    /* Executing the query and sort by name, in ascending order. */
    mongo::Query query = mongo::Query(queryObject).sort(usedName,-1);
    
    // -------------------------------
    
    std::auto_ptr<mongo::DBClientCursor> checkForAggregation = DB->query(dataGranulesTableMongoDB, query , N_TO_RETURN_1, N_TO_SKIP_0);
    
    bool isAggregation = checkForAggregation->next().getObjectField("adaguc").getField(getCurrentDimension()).Array().size() > 1;
    
    // -------------------------------
  
    mongo::BSONObjBuilder selectingBuilder;
    if (isAggregation) {
        selectingBuilder << usedName.c_str() << BSON("$slice" << -1) << "_id" << 0;
    } else {
        selectingBuilder << usedName.c_str() << 1 << "_id" << 0;
    }
    mongo::BSONObj selectingQuery = selectingBuilder.obj();
  
    /* MongoDB uses std::auto_ptr for getting all records. */
    std::auto_ptr<mongo::DBClientCursor> queryResultCursor;
    queryResultCursor = DB->query(dataGranulesTableMongoDB,query, N_TO_RETURN_1, N_TO_SKIP_0, &selectingQuery);
  
    std::string buff = name;
    buff.append(",");
    
    CDBStore::Store *maxStore = ptrToStore(queryResultCursor, buff.c_str(), 0);
  
    if(maxStore == NULL){
        setExceptionType(InvalidDimensionValue);
        CDBError("Invalid dimension value for  %s",name);
        CDBError("query failed"); 
        return NULL;
    }
    
    #ifdef MEASURETIME
        StopWatch_Stop("<CDBAdapterMongoDB::getMax");
    #endif
        
    return maxStore; 
}

/*
 * Getting unique values ordered by a selected value.
 * 
 * @param   const char*   The value to sort on.
 * @param     int       The limit for record to return.
 * @param   boolean     Ordered ascending or descending.
 * @param     const char*   The name or datasetpath.
 * @return    CDBStore::Store   A store containing the values ordered by value.
 */
CDBStore::Store *CDBAdapterMongoDB::getUniqueValuesOrderedByValue(const char *name, int limit, bool orderDescOrAsc,const char *table) {
    
    #ifdef MEASURETIME
        StopWatch_Stop(">CDBAdapterMongoDB::getUniqueValuesOrderedByValue");
    #endif
        
    mongo::DBClientConnection * DB = getDataBaseConnection();
    if(DB == NULL) {
        return NULL;
    }

    /* The corrected name. PostgreSQL columns are different compared to MongoDB fields.  */
    const char* correctedName = getCorrectedColumnName(name);
    std::string correctedNameAsString(correctedName);

    /* What do we want to select? Only the name variable. */
    mongo::BSONObjBuilder queryBSON;
    queryBSON << correctedName << 1 << "_id" << 0;
    mongo::BSONObj queryObj = queryBSON.obj();
  
    /* The query itself. */
    mongo::BSONObjBuilder theQuery;
    if(!hasEnding(table, "/")) {
        theQuery << "fileName" << table  << "dataSetName" << dataSetName << "dataSetVersion" << dataSetVersion;
    } else {
        theQuery << "adaguc.dataSetPath" << table << "dataSetName" << dataSetName << "dataSetVersion" << dataSetVersion; 
    }
    mongo::BSONObj queryAsObj = theQuery.obj();
  
    /* Making a query sorted by the selected value. */
    mongo::Query query = mongo::Query(queryAsObj).sort(correctedNameAsString.c_str(), orderDescOrAsc ? 1 : -1);

    std::auto_ptr<mongo::DBClientCursor> queryResultCursor;
    queryResultCursor = DB->query(dataGranulesTableMongoDB, query, limit, N_TO_SKIP_0, &queryObj);

    /* Again, only one column must be returned. */
    std::string columns = name;
    columns.append(",");
    
    // -------------------------------
    
    std::auto_ptr<mongo::DBClientCursor> checkForAggregation = DB->query(dataGranulesTableMongoDB, query , N_TO_RETURN_1, N_TO_SKIP_0);
    
    bool isAggregation = checkForAggregation->next().getObjectField("adaguc").getField(getCurrentDimension()).Array().size() > 1;
    
    // -------------------------------
  
    CDBStore::Store *store = ptrToStore(queryResultCursor, columns.c_str(), 0, isAggregation);

    if(store == NULL){
        CDBDebug("Query %s failed",query.toString().c_str());
    }
    
    #ifdef MEASURETIME
        StopWatch_Stop("<CDBAdapterMongoDB::getUniqueValuesOrderedByValue");
    #endif
        
    return store;
}

/*
 * Getting unique values ordered by a index.
 * NOTE: Only used if dimension is not time dimension.
 * 
 * @param   const char*   The value to sort on.
 * @param     int       The limit for record to return.
 * @param   boolean     Ordered ascending or descending.
 * @param     const char*   The name or datasetpath.
 * @return    CDBStore::Store   A store containing the values ordered by value.
 */
CDBStore::Store *CDBAdapterMongoDB::getUniqueValuesOrderedByIndex(const char *name, int limit, bool orderDescOrAsc,const char *table) {
    
    #ifdef MEASURETIME
        StopWatch_Stop(">CDBAdapterMongoDB::getUniqueValuesOrderedByIndex");
    #endif
        
    mongo::DBClientConnection * DB = getDataBaseConnection();
    if(DB == NULL) {
        return NULL;
    }

    /* The corrected name. Because of MongoDB, it can be that  */
    const char* correctedName = getCorrectedColumnName(name);
  
    std::string dimensionName = "dim";
    dimensionName.append(name);
    const char * correctedDimName = getCorrectedColumnName(dimensionName.c_str());
  
    /* What do we want to select? Only the name variable. */
    mongo::BSONObjBuilder queryBSON;
    queryBSON << correctedName << 1 << correctedDimName << 1 << "_id" << 0;
    mongo::BSONObj queryObj = queryBSON.obj();
  
    /* The query itself. */
    mongo::BSONObjBuilder queryItself;
    if(!hasEnding(table, "/")) {
        queryItself << "fileName" << table << "dataSetName" << dataSetName << "dataSetVersion" << dataSetVersion;
    } else {
        queryItself << "adaguc.dataSetPath" << table << "dataSetName" << dataSetName << "dataSetVersion" << dataSetVersion; 
    }
    mongo::BSONObj theQuery = queryItself.obj();
  
    /* Sorting on multiple fields. So creating a BSON */
    mongo::BSONObjBuilder sortingFieldsBuilder;
    sortingFieldsBuilder << correctedName << 1 << correctedDimName << 1;
    mongo::BSONObj sortingFields = sortingFieldsBuilder.obj();
  
    mongo::Query query = mongo::Query(theQuery).sort(sortingFields);
  
    std::auto_ptr<mongo::DBClientCursor> queryResultCursor = DB->query(dataGranulesTableMongoDB, query, limit, N_TO_SKIP_0, &queryObj);
  
    std::string columns = name;
    columns.append(",");
  
    CDBStore::Store *store = ptrToStore(queryResultCursor, columns.c_str(), 0);
  
    if(store == NULL){
        CDBDebug("Query %s failed",query.toString().c_str());
    }
    
    #ifdef MEASURETIME
        StopWatch_Stop("<CDBAdapterMongoDB::getUniqueValuesOrderedByIndex");
    #endif
        
    return store;
}

/*
 * Getting all the necessary data for different granules.
 * 
 * @param   CDataSource   A source containing all information from the XML and more.
 * @param   int       Limiting the results.
 * @return    CDBStore::Store   The result in Store form.
 */
CDBStore::Store *CDBAdapterMongoDB::getFilesAndIndicesForDimensions(CDataSource *dataSource,int limit) {
    
    #ifdef MEASURETIME
        StopWatch_Stop(">CDBAdapterMongoDB::getFilesAndIndicesForDimensions");
    #endif
  
    /* Getting the database connection. */
    mongo::DBClientConnection* DB = getDataBaseConnection();
    if(DB == NULL) {
        return NULL;
    }
  
    /* Setting the current used dimension for further use. */
    currentUsedDimension = dataSource->requiredDims[0]->netCDFDimName.c_str();
    CT::string queryParams(dataSource->requiredDims[0]->value);
  
    /* Getting the correct fileName or dataSetName. */
    CT::string tableName;
    try{
        tableName = getTableNameForPathFilterAndDimension(dataSource->cfgLayer->FilePath[0]->value.c_str(),dataSource->cfgLayer->FilePath[0]->attr.filter.c_str(), currentUsedDimension,dataSource);
    } catch(int e){
        CDBError("Unable to create tableName");
        return NULL;
    }
  
    /* Building the query. */
    mongo::BSONObjBuilder queryBuilder;
    if(!hasEnding(tableName.c_str(), "/")) {
        queryBuilder << "fileName" << tableName.c_str() << "dataSetName" << dataSetName << "dataSetVersion" << dataSetVersion;
    } else { 
        queryBuilder << "adaguc.dataSetPath" << tableName.c_str() << "dataSetName" << dataSetName << "dataSetVersion" << dataSetVersion;
    }
  
    /* The time must be without the 'T' and 'Z' characters, so erasing them. */
    const char* queryParamsChar = queryParams.c_str();
    std::string queryParamsString(queryParamsChar);
  
    boost::replace_all(queryParamsString, "T", " ");
    boost::erase_all(queryParamsString, "Z");

    /* If the time value has got a '/' character, it means a range.
     * Split it up and use $gte and $lt. */
    if (queryParamsString.find("/") != std::string::npos) {
        CT::string convertedToCtString(queryParamsString.c_str());
        CT::string* splittedString = convertedToCtString.splitToArray("/");
        queryBuilder << getCorrectedColumnName(dataSource->requiredDims[0]->netCDFDimName.c_str()) << mongo::GTE << splittedString[0].c_str() << mongo::LT << splittedString[1].c_str();
    } else {
        /* We don't have a time range. */
        queryBuilder << getCorrectedColumnName(dataSource->requiredDims[0]->netCDFDimName.c_str()) << queryParamsString.c_str();
    }
  
    mongo::BSONObj theQuery = queryBuilder.obj();
  
    mongo::BSONObjBuilder selectingBuilder;
    selectingBuilder << "adaguc.path" << 1 << getCorrectedColumnName(dataSource->requiredDims[0]->netCDFDimName.c_str()) << 1 << "_id" << 0;
  
    mongo::BSONObj selectingQuery = selectingBuilder.obj();
    
    /* --------------------------------- */
    /*   Getting the index (dimIndex).   */
    /* --------------------------------- */
    // The index is by default 0. ( for non aggregations )
    int dimIndex = 0;
    
    // Only need 1 field in return, the time field.
    mongo::BSONObjBuilder indexBuilder;
    indexBuilder << getCorrectedColumnName(dataSource->requiredDims[0]->netCDFDimName.c_str()) << 1 << "_id" << 0;
    mongo::BSONObj indexFieldSelector = indexBuilder.obj();
    
    // Executing the query, limiting the query to 1.
    std::auto_ptr<mongo::DBClientCursor> queryResultCursorAggregationCheck = DB->query(dataGranulesTableMongoDB, mongo::Query(theQuery), N_TO_RETURN_1, N_TO_SKIP_0, &indexFieldSelector);
    
    // Boolean for checking if the granule is an aggregation. By default false.
    bool isAggregation = false;
    
    if(queryResultCursorAggregationCheck->more()) {
        mongo::BSONObj value = queryResultCursorAggregationCheck->next();
        std::vector<mongo::BSONElement> dimensionValues = value.getObjectField("adaguc").getField(dataSource->requiredDims[0]->netCDFDimName.c_str()).Array();

        for(size_t index = 0; index < dimensionValues.size(); index++) {
            if(strcmp(dimensionValues[index].valuestr(),queryParamsString.c_str()) == 0) {
                isAggregation = true;
                dimIndex = index;
            }
        }
    }
    /* --------------------------------- */
  
    std::auto_ptr<mongo::DBClientCursor> queryResultCursor = DB->query(dataGranulesTableMongoDB, mongo::Query(theQuery).sort(getCorrectedColumnName(dataSource->requiredDims[0]->netCDFDimName.c_str()),-1), N_TO_RETURN_1, N_TO_SKIP_0, &selectingQuery);

    std::string labels = "path,";
    labels.append(dataSource->requiredDims[0]->netCDFDimName.c_str());
    labels.append(",dim");
    labels.append(dataSource->requiredDims[0]->netCDFDimName.c_str());

    CDBStore::Store *store = ptrToStore(queryResultCursor, labels.c_str(), dimIndex, isAggregation);
    
    #ifdef MEASURETIME
        StopWatch_Stop("<CDBAdapterMongoDB::getFilesAndIndicesForDimensions");
    #endif
        
    return store;
}

/*
 * Create a CDBSstore based on a subset of the result of the mongo query, bounded by the start, count and stride.
 *
 * Assumes that the mongo result is of the following BSON structure:
 * "adaguc": {
 *      "field1": "field_value1",
 *      "fieldx": "field_valuex",
 *      "dimension1:" [
 *          "dimension_value1.1",
 *          "dimension_value1.2",
 *          "dimension_value1.x"
 *      ]
 * }
 *
 * The dimension arrays are split over several records, which each contain all the fields, the dimension value and the dimension index.
 * For example, the mongo result above provides the following records:
 * field_value1, fieldvaluex, dimension_value1.1, 0
 * field_value1, fieldvaluex, dimension_value1.2, 1
 * field_value1, fieldvaluex, dimension_value1.x, 2
 * 
 * TODO: http://bvljira.knmi.nl/browse/KDCSP-175 - Also loop over and include other dimensions in the result. Currently we only use the first one (time).
 * For the current OPeNDAP implementation this is not a problem, since time is the only variable which we can aggregate over multiple files.
 * TODO: In the current implementation the stride is not used yet. This is a generic issue for all database adapters.
 *
 * @param   fields      All the fields which should be added to store records
 * @param   dimensions  All the dimensions over which we should loop and which should be added to the store
 * @param   queryResultCursor  The query result, should consist of all available granules for a dataset.
 * @param   start       The start index for each dimension
 * @param   count       The amount of required results for each dimension
 * @param   stride      The stride for each dimension
 * @return    CDBStore::Store   The result in Store form.
 */
CDBStore::Store *splitDimensionsToStoreRecords(std::vector<std::string> fields,std::vector<std::string> dimensions,std::auto_ptr<mongo::DBClientCursor> queryResultCursor,size_t *start,size_t *count,ptrdiff_t *stride) {

    // Initialize the column model. We need to add twice the dimension size because we will be adding both the dimension value and index.
    CDBStore::ColumnModel *colModel = new CDBStore::ColumnModel(fields.size() + 2*dimensions.size());
    CDBStore::Store *store=new CDBStore::Store(colModel);

    int indexFirstDim = 0;
    bool finished = false;

    // Loop over all the granules.
    while (queryResultCursor->more() && !finished) {
        mongo::BSONObj granuleObj = queryResultCursor->next();

        #ifdef CDBAdapterMongoDB_DEBUG
        CDBDebug("GranuleObj = %s", granuleObj.toString().c_str());
        CDBDebug("indexFirstDim = %d", indexFirstDim);
        CDBDebug("dimensions[0] = %s", dimensions[0].c_str());
        #endif

        // Check if all indices in this granule are still lower than the start index.
        // TODO: Also include other dimensions in this check.
        std::vector<mongo::BSONElement> firstDimension = granuleObj.getObjectField("adaguc").getField(dimensions[0]).Array();
        if (indexFirstDim + firstDimension.size() < start[0]) {
            indexFirstDim = indexFirstDim + firstDimension.size();
            #ifdef CDBAdapterMongoDB_DEBUG
            CDBDebug("Skipped this granule.");
            #endif
            continue;
        }

        // The aggregation is part of in this file.
        // For the first granule, we might need to skip a few steps.
        int indexGranuleDimension = 0;
        if (indexFirstDim < start[0]) {
              #ifdef CDBAdapterMongoDB_DEBUG
              CDBDebug("Skipped dimension steps %d untill %d.", indexFirstDim, start[0]);
              #endif
              indexGranuleDimension = start[0] - indexFirstDim;
              indexFirstDim = start[0];
        }

        for (int i = indexGranuleDimension; i < firstDimension.size(); i++){

            // If we are past the number of required steps, we can stop.
            // TODO: Also include other dimensions in this check.
            if (indexFirstDim >= count[0] + start[0]) {
                finished = true;
                #ifdef CDBAdapterMongoDB_DEBUG
                CDBDebug("Finished processing the granules at dimension step %d.", indexFirstDim);
                #endif
                break;
            }

            // We need to process this dimension step.
            CDBStore::Record *record = new CDBStore::Record(colModel);

            // Push all the necessary fields to the store.
            int store_index = 0;
            for (int field_index = 0; field_index < fields.size(); field_index++) {
                std::string fieldValue = granuleObj.getObjectField("adaguc").getStringField(fields[field_index]);
                record->push(store_index,fieldValue.c_str());
                store_index++;
            }

            // Get dimension value and required dimension index and push them to the store.
            // TODO: Also loop over other required dimensions.
            record->push(store_index,firstDimension[i].str().c_str());
            record->push(store_index+1,numberToString(i).c_str());
            store->push(record);

            indexFirstDim++;
        }
    }

    return store;
}

/*
 * Retrieves file path, dimension value and dimension index for all required dimensions.
 * TODO: In the current implemenation we always use the first required dimension. Please see the issues KDCSP-173, KDCSP-174, KDCSP-175.
 * For the current OPeNDAP implementation this is not a problem, since time is the only variable which we can aggregate over multiple files.
 * TODO: In the current implementation the stride and the limit are not used yet. This is a generic issue for all database adapters.
 *
 * @return    CDBStore::Store   The result in Store form.
 */
CDBStore::Store *CDBAdapterMongoDB::getFilesForIndices(CDataSource *dataSource,size_t *start,size_t *count,ptrdiff_t *stride,int limit) {

    #ifdef MEASURETIME
    StopWatch_Stop(">CDBAdapterMongoDB::getFilesForIndices");
    #endif

    mongo::DBClientConnection * DB = getDataBaseConnection();
    if(DB == NULL) {
        return NULL;
    }

    // -------------------------------------------------------------------------------------
    // Determine the filter for the granules, either based on path or datasetPath.
    // -------------------------------------------------------------------------------------
    mongo::BSONObjBuilder query;

    const char* filePath = dataSource->cfgLayer->FilePath[0]->value.c_str();

    if(!hasEnding(filePath, "/")) {
        query << "adaguc.path" << filePath << "dataSetName" << dataSetName << "dataSetVersion" << dataSetVersion;
    } else {
        query << "adaguc.dataSetPath" << filePath << "dataSetName" << dataSetName << "dataSetVersion" << dataSetVersion;
    }
    mongo::BSONObj queryObjBSON = query.obj();

    // -------------------------------------------------------------------------------------
    // Determine the fields to select and the sort order of the query.
    // -------------------------------------------------------------------------------------
    mongo::BSONObjBuilder desiredFieldsBuilder;
    desiredFieldsBuilder << "adaguc.path" << 1 << "_id" << 0;
    mongo::BSONObjBuilder sortOrderBuilder;

    for(size_t i=0;i<dataSource->requiredDims.size();i++){

        // Add all the desired dimensions to the seleciton.
        std::string requiredDimensionField = "adaguc.";
        std::string requiredDimensionName = dataSource->requiredDims[i]->netCDFDimName.c_str();
        requiredDimensionField.append(requiredDimensionName);
        desiredFieldsBuilder << requiredDimensionField.c_str() << 1;

        // Add all the required dimensions to the sort order.
        sortOrderBuilder << requiredDimensionField.c_str() << 1;
    }

    mongo::BSONObj desiredFieldsObjBSON = desiredFieldsBuilder.obj();

    // -------------------------------------------------------------------------------------
    // Select the granules and build the CDBStore.
    // -------------------------------------------------------------------------------------
    std::auto_ptr<mongo::DBClientCursor> queryResultCursor = DB->query(dataGranulesTableMongoDB, mongo::Query(queryObjBSON).sort(sortOrderBuilder.obj()), N_TO_RETURN_0, N_TO_SKIP_0, &desiredFieldsObjBSON);

    #ifdef CDBAdapterMongoDB_DEBUG
    CDBDebug("Start[0] = %d, count[0] = %d, stride[0] = %d", start[0], count[0], stride[0]);
    #endif

    // Create a vector of all the dimensions names which should be used to create the store.
    std::vector<std::string> dimensions;
    for(size_t i=0;i<dataSource->requiredDims.size();i++){
        dimensions.push_back(dataSource->requiredDims[i]->netCDFDimName.c_str());
    }

    // Create a vector of all the fields which should be added to the store.
    std::vector<std::string> fields;
    fields.push_back("path");

    // Create a store which contains a subset of the result based on the start, count and stride and where the dimension arrays are flattened into separate records.
    CDBStore::Store *store = splitDimensionsToStoreRecords(fields,dimensions, queryResultCursor, start, count, stride);

    #ifdef MEASURETIME
    StopWatch_Stop("<CDBAdapterMongoDB::getFilesForIndices");
    #endif

    return store;
}

/*
 * Getting all the necessary data for different granules.
 * 
 * @param   const char*   The filename or dataSetName.
 * @param   const char*   The layer that is being used.
 * @return    CDBStore::Store   The store containing the results.
 */
CDBStore::Store *CDBAdapterMongoDB::getDimensionInfoForLayerTableAndLayerName(const char *layertable,const char *layername) {
    
    #ifdef MEASURETIME
        StopWatch_Stop(">CDBAdapterMongoDB::getDimensionInfoForLayerTableAndLayerName");
    #endif
  
    /* First getting the database connection. */
    mongo::DBClientConnection * DB = getDataBaseConnection();
    if(DB == NULL) {
        return NULL;
    }
  
    /* Selecting the granule with the specific fileName. */
    mongo::BSONObjBuilder query;
    if(!hasEnding(layertable, "/")) {
        query << "fileName" << layertable << "dataSetName" << dataSetName << "dataSetVersion" << dataSetVersion;
    } else { 
        query << "adaguc.dataSetPath" << layertable << "dataSetName" << dataSetName << "dataSetVersion" << dataSetVersion;
    }
    mongo::BSONObj objBSON = query.obj();
  
    /* Figure out what dimension is being used. 
     * This is stored in the dataSets collection. */
    /* Getting the dimension. */
    mongo::BSONObjBuilder dataSetBuilder;
    dataSetBuilder << "dataSetName" << dataSetName << "dataSetVersion" << dataSetVersion;
    mongo::BSONObj dataSetQuery = dataSetBuilder.obj();
  
    mongo::BSONObjBuilder dataSetSelecting;
    dataSetSelecting << "dimension" << 1 << "_id" << 0;
    mongo::BSONObj dataSetSelectingObj = dataSetSelecting.obj();
  
    /* Executing the query. */
    std::auto_ptr<mongo::DBClientCursor> ptrForDimension = DB->query(dataSetsTableMongoDB, mongo::Query(dataSetQuery), N_TO_RETURN_1, N_TO_SKIP_0, &dataSetSelectingObj);
    
    const char* dimensionName;
    if(ptrForDimension->more()) {
        dimensionName = ptrForDimension->next().getStringField("dimension"); 
    } else {
        dimensionName = ""; 
        return NULL;
    }
  
    /* Selecting the fields that must be returned. Named:
     * layer, ncname, ogcname and units. */
    // The layer name, so getting it from the config file.
    const char* layerName = configurationObject->Layer[3]->Variable[0]->value.c_str();
    // The ncname is in fact the dimensionName.
    const char* ncName = dimensionName;
    // The ogcname can be different compared to the ncname, so getting it out of the db.
    
    std::string ogcNameAsString;
    ogcNameAsString.append("adaguc.layer.");
    ogcNameAsString.append(layername);
    ogcNameAsString.append(".dimension");
    const char* ogcname_name = ogcNameAsString.c_str();
    // Units is just declared in the 
    const char* units = "adaguc.units";
  
    currentUsedDimension = ncName;

    mongo::BSONObjBuilder selectingColumns;
    selectingColumns << layerName << 1 << ogcname_name << 1 << units << 1 << "_id" << 0;
    mongo::BSONObj selectedColumns = selectingColumns.obj();
  
    mongo::BSONObjBuilder selectingCorrectGranule;
    if(!hasEnding(layertable, "/")) {
        selectingCorrectGranule << "fileName" << layertable << "dataSetName" << dataSetName << "dataSetVersion" << dataSetVersion;
    } else { 
        selectingCorrectGranule << "adaguc.dataSetPath" << layertable << "dataSetName" << dataSetName << "dataSetVersion" << dataSetVersion;
    }
    mongo::BSONObj selectingCorrectGranuleObj = selectingCorrectGranule.obj();

    /* Collecting the results. */
    std::auto_ptr<mongo::DBClientCursor> queryResultCursor;
    queryResultCursor = DB->query(dataGranulesTableMongoDB,mongo::Query(selectingCorrectGranuleObj), N_TO_RETURN_1, N_TO_SKIP_0, &selectedColumns);

    /* Making a store of it. */
    CDBStore::Store *store = ptrToStore(queryResultCursor, tableCombi.find("autoconfigure_dimensions")->second.c_str(), 0);
  
    if(store==NULL){
        CDBDebug("No dimension info stored for %s",layername);
    }
    
    #ifdef MEASURETIME
        StopWatch_Stop("<CDBAdapterMongoDB::getDimensionInfoForLayerTableAndLayerName");
    #endif
        
    return store;
}

/*
 * Storing dimension info about granules into the MongoDB database.
 */
int CDBAdapterMongoDB::storeDimensionInfoForLayerTableAndLayerName(const char *layertable,const char *layername,const char *netcdfname,const char *ogcname,const char *units) {
    
    #ifdef MEASURETIME
        StopWatch_Stop(">CDBAdapterMongoDB::storeDimensionInfoForLayerTableAndLayerName");
    #endif
    
    /* First getting the database connection. */
    mongo::DBClientConnection * DB = getDataBaseConnection();
    if(DB == NULL) {
        return -1;
    }
  
    /* Selecting the right granule. */
    mongo::BSONObjBuilder query;
    query << "fileName" << layertable << "dataSetName" << dataSetName << "dataSetVersion" << dataSetVersion;
    mongo::BSONObj selectedGranule = query.obj();
   
    CT::string updateString = "adaguc.layer.";
    updateString.concat(layername);
    updateString.concat(".dimension.");
    updateString.concat(netcdfname);
    updateString.concat(".ogcname");
   
    mongo::BSONObjBuilder queryCheck;
    queryCheck << "$set" << BSON("adaguc.units" << units << updateString.c_str() << ogcname);
    mongo::BSONObj objBSONCheck = queryCheck.obj();
   
    /* Update the right granule. */
    DB->update(dataGranulesTableMongoDB, mongo::Query(selectedGranule), objBSONCheck);
  
    #ifdef MEASURETIME
        StopWatch_Stop("<CDBAdapterMongoDB::storeDimensionInfoForLayerTableAndLayerName");
    #endif
        
    return 0;
}

/*
 * Probably not used.
 */
int CDBAdapterMongoDB::removeDimensionInfoForLayerTableAndLayerName(const char *layertable,const char *layername) {
    
    #ifdef MEASURETIME
        StopWatch_Stop(">CDBAdapterMongoDB::removeDimensionInfoForLayerTableAndLayerName");
    #endif
        
    mongo::DBClientConnection * DB = getDataBaseConnection();
    if(DB == NULL) {
        return -1;
    }
    
    mongo::BSONObjBuilder query;
    query << "fileName" << layertable;
    mongo::BSONObj objBSON = query.obj();
    
    /* When it's being used, implement it! */
    //DB->remove(dataGranulesTableMongoDB,mongo::Query(objBSON),true);
    
    if(DB->getPrevError().isEmpty()) {
        throw(__LINE__);
    }
    
    #ifdef MEASURETIME
        StopWatch_Stop("<CDBAdapterMongoDB::removeDimensionInfoForLayerTableAndLayerName");
    #endif
        
    return 0;
}
    
/*
 * Not used anymore. Must be kept, because ADAGUC expects it exists.
 */
int CDBAdapterMongoDB::dropTable(const char *tablename) {
    
    mongo::DBClientConnection * DB = getDataBaseConnection();
    if(DB == NULL) {
        return -1;
    }
    
    return 0; 
}

int CDBAdapterMongoDB::createDimTableOfType(const char *dimname,const char *tablename,int type) {
    
    #ifdef MEASURETIME
        StopWatch_Stop(">CDBAdapterMongoDB::createDimTableOfType");
    #endif
        
    mongo::DBClientConnection * DB = getDataBaseConnection();
    if(DB == NULL) {
        return -1;
    }
  
    CT::string tableColumns("path varchar (511)");
   
    if(type == 3)tableColumns.printconcat(", %s timestamp, dim%s int",dimname,dimname);
    if(type == 2)tableColumns.printconcat(", %s varchar (64), dim%s int",dimname,dimname);
    if(type == 1)tableColumns.printconcat(", %s real, dim%s int",dimname,dimname);
    if(type == 0)tableColumns.printconcat(", %s int, dim%s int",dimname,dimname);
  
    tableColumns.printconcat(", filedate timestamp");
    tableColumns.printconcat(", PRIMARY KEY (path, %s)",dimname);
  
    int status = 0;
  
    status = checkTableMongo(tablename,tableColumns.c_str());
  
    #ifdef MEASURETIME
        StopWatch_Stop("<CDBAdapterMongoDB::createDimTableOfType");
    #endif
        
    return status;
}

int CDBAdapterMongoDB::createDimTableInt(const char *dimname,const char *tablename) {
    return createDimTableOfType(dimname,tablename,0);
}

int CDBAdapterMongoDB::createDimTableReal(const char *dimname,const char *tablename) {
    return createDimTableOfType(dimname,tablename,1);
}

int CDBAdapterMongoDB::createDimTableString(const char *dimname,const char *tablename) {
    return createDimTableOfType(dimname,tablename,2);
}

int CDBAdapterMongoDB::createDimTableTimeStamp(const char *dimname,const char *tablename) {
    return createDimTableOfType(dimname,tablename,3);
}

/*
 * Lookup for the full path of one granule of a certain dataset.
 * It's a replacement for a full directory search OPeNDAP uses to get one header out of one file.
 * @param   const char*     The name of the dataset
 * @param   const char*     The version of the dataset
 */
const char* CDBAdapterMongoDB::firstGranuleLookup(const char* datasetName, const char* datasetVersion) {

    mongo::DBClientConnection * DB = getDataBaseConnection();
    if(DB == NULL) {
        return NULL;
    }

    /*
     * Compose the query.
     */
    mongo::BSONObjBuilder query;
    query << "dataSetName" << datasetName << "dataSetVersion" << datasetVersion << "adaguc" << BSON("$exists" << "true");
    mongo::BSONObj objBSON = query.obj();

    /*
     * Select the fields that are wanted.
     */
    mongo::BSONObjBuilder desiredFieldsBuilder;
    desiredFieldsBuilder << "adaguc.path" << 1 << "_id" << 0;
    mongo::BSONObj desiredFieldsObjBSON = desiredFieldsBuilder.obj();

    /* Executing the query, only return 1 result. */
    std::auto_ptr<mongo::DBClientCursor> cursorFromMongoDB = DB->query(dataGranulesTableMongoDB, mongo::Query(objBSON), 1, 0, &desiredFieldsObjBSON);

    if (cursorFromMongoDB->more()) {
        /* Extract the field and return it. */
        std::string fullPath = cursorFromMongoDB->next().getObjectField("adaguc").getStringField("path");
        return fullPath.c_str();
    } else {
        return NULL;
    }
}

/*
 * Checking if the files exists in the MongoDB collection.
 * 
 * @param   const char*   The filename or dataSetName.
 * @param   const char*   The layer that is being used.
 * @return    int     The result. 1 for doesn't exist, 0 for exists!
 */
int CDBAdapterMongoDB::checkIfFileIsInTable(const char *tablename,const char *filename) {
    
    #ifdef MEASURETIME
        StopWatch_Stop(">CDBAdapterMongoDB::checkIfFileIsInTable");
    #endif
        
    mongo::DBClientConnection * DB = getDataBaseConnection();
    if(DB == NULL) {
        return -1;
    }
  
    /* Granule needs to have the correct fileName and the correct path. */
    mongo::BSONObjBuilder query;
    query << "fileName" << tablename << "adaguc.path" << filename << "dataSetName" << dataSetName << "dataSetVersion" << dataSetVersion;
    mongo::BSONObj objBSON = query.obj();
  
    /* Converting the path from the granule to the datasetpath. */
    CT::string fileNameAsString(filename);
    char * file = (char*) filename;
  
    int position = strpos(file, "/", 8);
    fileNameAsString.substringSelf(&fileNameAsString, 0, position);
    fileNameAsString.concat("/");
  
    /* Writing it once! */
    if(!configWritten) {
        writeConfigXMLToDatabase(filename,fileNameAsString.c_str());
        configWritten = true;
    }
  
    /* Third parameter is number of results. */
    std::auto_ptr<mongo::DBClientCursor> queryResultCursor = DB->query(dataGranulesTableMongoDB, mongo::Query(objBSON));
  
    /* If there is result, and the next one is valid ( so no $err.. ) */
    int fileIsOK = 1;
    if(queryResultCursor->more()) {
        if(queryResultCursor->next().isValid()) {
            fileIsOK=0;  
        } else {
            fileIsOK=1;
        }
    } else {
        fileIsOK=1;
    }
    
    #ifdef MEASURETIME
        StopWatch_Stop("<CDBAdapterMongoDB::checkIfFileIsInTable");
    #endif
  
    return fileIsOK; 
}
    
/*
 * Removing a file.
 * 
 * @param   const char*     The name of the table ( which is always the same ).
 * @param   const char*     The file to remove ( full path ).
 */
int CDBAdapterMongoDB::removeFile(const char *tablename,const char *file) {
    
    #ifdef MEASURETIME
        StopWatch_Stop(">CDBAdapterMongoDB::removeFile");
    #endif
        
    mongo::DBClientConnection * DB = getDataBaseConnection();
    if(DB == NULL) {
        return -1;
    }
  
    mongo::BSONObjBuilder query;
    query << "path" << file;
    mongo::BSONObj objBSON = query.obj();
    /* Third param is true, this means after one record has been found, it stops. */
    //DB->remove(tablename,mongo::Query(objBSON),true);
  
    if(DB->getPrevError().isEmpty()) {
        throw(__LINE__);
    }
    
    #ifdef MEASURETIME
        StopWatch_Stop("<CDBAdapterMongoDB::removeFile");
    #endif
        
    return 0;
}

/*
 * Remove files with changed creation dates.
 */
int CDBAdapterMongoDB::removeFilesWithChangedCreationDate(const char *tablename,const char *file,const char *creationDate) {
    
    #ifdef MEASURETIME
        StopWatch_Stop(">CDBAdapterMongoDB::removeFilesWithChangedCreationDate");
    #endif
        
    mongo::DBClientConnection * DB = getDataBaseConnection();
    if(DB == NULL) {
        return -1;
    }
  
    mongo::BSONObjBuilder query;
    query << "path" << file << "filedate" << mongo::NE << creationDate;
    mongo::BSONObj objBSON = query.obj();
  
    std::string buff("database.");
    buff.append(tablename);  
  
    //DB->remove(buff,mongo::Query(objBSON)); 
    if(DB->getPrevError().isEmpty()) {
        throw(__LINE__);
    }
    
    #ifdef MEASURETIME
        StopWatch_Stop("<CDBAdapterMongoDB::removeFilesWithChangedCreationDate");
    #endif
        
    return 0; 
}

int CDBAdapterMongoDB::setFileInt(const char *tablename,const char *file,int dimvalue,int dimindex,const char*filedate,GeoOptions *geoOptions) {
    CT::string values;
    values.print("('%s',%d,'%d','%s')",file,dimvalue,dimindex,filedate);
  
    #ifdef CDBAdapterMongoDB_DEBUG
        CDBDebug("Adding INT %s",values.c_str());
    #endif
        
    fileListPerTable[tablename].push_back(values.c_str());
    return 0;
}

int CDBAdapterMongoDB::setFileReal(const char *tablename,const char *file,double dimvalue,int dimindex,const char*filedate,GeoOptions *geoOptions) {
    CT::string values;
    values.print("('%s',%f,'%d','%s')",file,dimvalue,dimindex,filedate);
    #ifdef CDBAdapterMongoDB_DEBUG
        CDBDebug("Adding REAL %s",values.c_str());
    #endif
        
    fileListPerTable[tablename].push_back(values.c_str());
    return 0;
}

int CDBAdapterMongoDB::setFileString(const char *tablename,const char *file,const char * dimvalue,int dimindex,const char*filedate,GeoOptions *geoOptions) {
    CT::string values;
    values.print("('%s','%s','%d','%s')",file,dimvalue,dimindex,filedate);
  
    #ifdef CDBAdapterMongoDB_DEBUG
        CDBDebug("Adding STRING %s",values.c_str());
    #endif
    
    fileListPerTable[tablename].push_back(values.c_str());
    return 0;
}

int CDBAdapterMongoDB::setFileTimeStamp(const char *tablename,const char *file,const char *dimvalue,int dimindex,const char*filedate,GeoOptions *geoOptions) {
    CT::string values;
    values.print("('%s','%s','%d','%s')",file,dimvalue,dimindex,filedate);
  
    #ifdef CDBAdapterMongoDB_DEBUG
        CDBDebug(" Adding TIMESTAMP %s",values.c_str());
    #endif
    
    fileListPerTable[tablename].push_back(values.c_str());
    return 0;
}

/*
 * Adding information about granules to the MongoDB database.
 */
int CDBAdapterMongoDB::addFilesToDataBase() {
    
    #ifdef MEASURETIME
        StopWatch_Stop(">CDBAdapterMongoDB::addFilesToDataBase");
    #endif
        
    /* First getting the database connection. */
    mongo::DBClientConnection * DB = getDataBaseConnection();
    if(DB == NULL) {
        return -1;
    }

    for (std::map<std::string,std::vector<std::string> >::iterator  it=fileListPerTable.begin(); it!=fileListPerTable.end(); ++it){
        if(it->second.size()>0) {
      
            /* Updating record with corresponding info. */
            CT::string pathOfGranule = it->second[0].c_str();
            CT::string dataSetNameString = pathOfGranule.splitToArray("/")[6].c_str();
        
            mongo::BSONObjBuilder selected;
            selected << "fileName" << it->first.c_str() << "dataSetName" << dataSetNameString.c_str() << "dataSetVersion" << dataSetVersion;
            mongo::BSONObj selectedGranule = selected.obj();
            /* ---------------------------------------- */
        
            CT::string* arrayOfElementsToWrite;
            mongo::BSONObjBuilder queryBuilder;
            
            /* This is the information of the whole granule. */
            CT::string tmpCurrentGranule = it->second[0].c_str();
            tmpCurrentGranule.substringSelf(1,tmpCurrentGranule.length()-1);
            arrayOfElementsToWrite = tmpCurrentGranule.splitToArray(",");
            
            /* The path of the granule. */
            CT::string path = *(arrayOfElementsToWrite);
            path.substringSelf(1,path.length()-1);
            
            /* The file date of the granule. */
            CT::string fileDate = *(arrayOfElementsToWrite + 3);
            fileDate.substringSelf(1,fileDate.length()-2);
            fileDate.replaceSelf("T", " ");
            
            CT::string usedDimensionToStore = "adaguc.";
            usedDimensionToStore.concat(currentUsedDimension);
                
            /* And infally the dataset path. */
            char* dataSetPath = (char*) path.c_str();
            int position = strpos(dataSetPath, "/", 8);
            CT::string dataSetPathString = path;
            dataSetPathString.substringSelf(&dataSetPathString, 0, position);
            dataSetPathString.concat("/");

            mongo::BSONObjBuilder subQueryBuilder;
            subQueryBuilder << "adaguc.path" << path.c_str() << "adaguc.dataSetPath" << dataSetPathString.c_str() << "adaguc.filedate" << fileDate.c_str();
        
            /* Getting all the time values. */
            std::vector<std::string> builder;
            for(size_t j=0;j<it->second.size();j++){
                CT::string tmp = it->second[j].c_str();
                
                tmp.substringSelf(1,tmp.length()-1);
                arrayOfElementsToWrite = tmp.splitToArray(","); 
            
                /* Getting the neccesary information. */
                CT::string dimensionValue = *(arrayOfElementsToWrite + 1);
                dimensionValue.substringSelf(1,dimensionValue.length()-2);
                
                dimensionValue.replaceSelf("T", " ");

                if(std::find(builder.begin(), builder.end(), dimensionValue.c_str()) != builder.end()) {
                    if(!conditionalSecondDimension.equals("")) {
                        CT::string secondField = "adaguc.";
                        secondField.concat(conditionalSecondDimension.c_str());
                        std::vector<std::string> builderForSecondDimension;
                        builderForSecondDimension.push_back(dimensionValue.c_str());

                        subQueryBuilder << secondField.c_str() << builderForSecondDimension;
                    }
                } else {
                    builder.push_back(dimensionValue.c_str());
                }
            }

            subQueryBuilder << usedDimensionToStore.c_str() << builder;

            queryBuilder << "$set" << subQueryBuilder.obj();
            mongo::BSONObj objBSON = queryBuilder.obj();
            
            /* Update the right dataset. */
            DB->update(dataGranulesTableMongoDB, mongo::Query(selectedGranule), objBSON);
        }
    
    }
  
    fileListPerTable.clear();
    
    #ifdef MEASURETIME
        StopWatch_Stop("<CDBAdapterMongoDB::addFilesToDataBase");
    #endif
    
    return 0;
}
#endif
