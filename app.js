//
// Splits a FISERV TAX document batch into folders with a maximum of N documents in each one
//
// Node async fs: https://2ality.com/2019/11/nodejs-streams-async-iteration.html
//
const fs = require('fs');
const readline = require('readline');
//const minimist = require('minimist');
const { once } = require('events');

const util = require('util');
const stream = require('stream');
const finished = util.promisify(stream.finished);

const BATCH_SIZE = 9999;
//const BATCH_SIZE = 9;

const INPUT_FOLDER = '//s617985ch3nas01/ENCORE_Import_Prod/PwrLdr_Fiserv_Originate/Taxes/';
const OUTPUT_FOLDER = '//s617985ch3nas01/ENCORE_Import_Prod/PwrLdr_Fiserv_Originate/_Taxes/';

const INDEX_FILE = '428.66820000_IRS_DA_0015_01_Archive_Inst_428__1099-INT.txt';
const BATCH_FOLDER_BASE = 'FISERV_ORIG_BATCH_';

let batchIdentifier = 0;

const getOutputFolderName = (_outputFolder, _batchFolderBase, _batchIdentifier) => {
    return [(_outputFolder + _batchFolderBase + _batchIdentifier.toString().padStart(2, '0')), _batchFolderBase + _batchIdentifier.toString().padStart(2, '0') + '.idx'];
}

const createFolder = async (folderPath) => {
    try {
        await fs.promises.mkdir(folderPath);
    } catch(ex) {
        console.error('Error:', ex);
    }
}

const copyFile = async (fromFolder, toFolder, fileName) => {
    try {
        await fs.promises.copyFile(fromFolder + fileName, toFolder + '/' + fileName);
    } catch(ex) {
        console.error('Copy file error:', ex);
    }
}


const processLineByLine = async (filePath, batchSize) => {
    const rli = readline.createInterface({
      input: fs.createReadStream(filePath),
      crlfDelay: Infinity
    });
  
    let batchFileCount = 0;
    let newFolderPath = null;
    let idxFileName = null;
    let writeStream = null;

    for await (const line of rli) {
      //console.log(line);
      const [docType, ssn, customerName, docData, acctNum, docName] = line.split('|');

      if (batchFileCount === 0) {
          [newFolderPath, idxFileName] = getOutputFolderName(OUTPUT_FOLDER, BATCH_FOLDER_BASE, batchIdentifier++);
          console.log('Creating new output folder:', newFolderPath);
          createFolder(newFolderPath);

          // Create new output stream for the index file
          if (writeStream){
              writeStream.end();
              await finished(writeStream);
          }

          try {
            writeStream = fs.createWriteStream(newFolderPath + '/' + idxFileName, {encoding: 'utf8'});
          } catch(error) {
              console.error('ERROR in createWriteStream:', error);
              process.exit(1);
          }

      } else if (batchFileCount >= BATCH_SIZE) {
        batchFileCount = -1;
      }

      batchFileCount++;

      for await (const chunk of [line, '\n']) {
        if (!writeStream.write(chunk)) {
            await once(writeStream, 'drain');
        }
      }
      copyFile(INPUT_FOLDER, newFolderPath, docName);
    }
}

processLineByLine(INPUT_FOLDER + INDEX_FILE, BATCH_SIZE)
    .catch(error => console.error('ERROR:', error));
