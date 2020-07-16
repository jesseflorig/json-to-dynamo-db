const path = require('path')
const fs = require('fs')
const { exec } = require('child_process')

const srcDirectory = path.join(__dirname, 'src')
const outDirectory = path.join(__dirname, 'out')

// Read the source directory
fs.readdir(srcDirectory, (err, files) => {
  if(err){
    return console.error(`Unable to scan "${srcDirectory}": ${err}`)
  }

  // Filter json files
  const jsonFiles = files.filter(file => file.toLowerCase().endsWith('.json'))
  const jsonCount = jsonFiles.length
  console.log(`Found ${jsonCount} json files...`)

  jsonFiles.forEach((file, fileIdx) => {
    const filePath = path.join(srcDirectory, file)
    const outPath = path.join(outDirectory, file)
    const tableName = file.slice(0, file.length - 5)

    console.log(`Converting "${file}" (${fileIdx} of ${jsonCount})...`)

    // Convert to batch items
    fs.readFile(filePath, 'utf-8', (err, data) => {
      if(err){
        return console.error(`Unable to read "${file}": ${err}`)
      }

      const jsonData = JSON.parse(data)
      const batchItems = jsonData.map(item => {
        // Generate ID if it doesnt exist
        item.id = item.id || item.name.toLowerCase().replace(/\s/g, '-')

        const itemKeys = Object.keys(item)
        
        // Try to determine the best data type
        itemKeys.map(key => {
          const val = item[key]
          if(typeof val === "string"){
            item[key] = { "S" : val }
          }
          else if(typeof val === "number"){
            item[key] = { "N" : val.toString() }
          }
          else if(typeof val === "boolean"){
            item[key] = { "BOOL" : val }
          }
          // May only work on a list of objects
          else if(Array.isArray(val)){
            item[key] = { "L" : [ ] }
            val.map((listItem, idx) => {
              const attrKeys = Object.keys(listItem)
              const attrs = {}
              attrKeys.map(attr => {
                attrs[attr] = { "S": listItem[attr] }
              })
              item[key]["L"].push({
                "M":  attrs 
              })
            })
          }
          else {
            console.warn(`Unhandled value type ${typeof val} for "${val}" - Converting to string`)
            item[key] = { "S" : `${val}` }
          }
        })

        return {
          "PutRequest": {
            "Item": item
          }
        }
      })

      // TODO: Figure out why I have to slice to get this to work 
      // (succesfully did 20 at a time through all 207 test items)
      const batchData = `{ "${tableName}" : ${JSON.stringify(batchItems.slice(0, 20))} }`

      //Write the batch write json file
      fs.writeFile(outPath, batchData, (err) => {
        if(err){
          return console.error(`Unable to write "${outPath}": ${err}`)
        }
        
        // Execute AWS Batch write
        const awsCommand = `aws dynamodb batch-write-item --request-items file://${outPath}`
        exec(awsCommand, (err, stdout, stderr) => {
          console.log(`Pushing "${tableName}" table data (${fileIdx} of ${jsonCount})...`)
          if(err){
            return console.error(`Error executing "${awsCommand}": ${err}`)
          }

          console.log(stdout)
          console.error(stderr)
          console.log(`Finished pushing ${jsonCount} tables!`)
        })
      })
    })
  })
})

