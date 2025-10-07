// import mongo client
const { MongoClient} = require('mongodb');

// connection  uri
const uri = 'mongodb://localhost:27017';



async function queries(){
    const client = new MongoClient(uri);

    try{
        await client.connect();
        console.log("Connected to the MongoDB server successfuly!!!!");

        //accessing the database and collection
        const db = client.db('plp_bookstore');
        const collection = db.collection('books');

                    //****queries

        //1. Find all books in a specific genre
        console.log("\n Books in fantasy genre");
        const fantasyBooks = await collection.find({genre: "Fantasy"}).toArray();
        console.log(fantasyBooks);

        //2. Find books published after a certain year
         console.log("\n Books after 2012");
        const booksAfter = await collection.find({published_year: {$gt: 2012}}).toArray();
        console.log(booksAfter);

        //3. Find books by a specific author
         console.log("\n Books written by Madeline Miller");
        const booksByMadeline = await collection.find({author: "Madeline Miller"}).toArray();
        console.log(booksByMadeline);

        //4. Update the price of a specific book
        const educatedNewPrice = await collection.updateOne(
            {title: "Educated"},
            {$set: {price: 10.99}}
        );

        //5. Delete a book by its title
        const deleteBook = await collection.deleteOne({title: "Wuthering Heights"});

        
                     //****Advanced queries

        //1. Write a query to find books that are both in stock and published after 2010
        console.log("\n Books in Stock and published after 2010");
        const findBooks = await collection.find({$and: [
            {in_stock: true}, 
            {published_year: {$gt: 2010}}
        ]}).toArray();
        console.log(findBooks);

        //2. Use projection to return only the title, author, and price fields in your queries
        console.log("\n Books through projection");
        const projection = await collection.find({}, {projection: {_id: 0, title: 1, author: 1, price: 1}}).toArray();
        console.log(projection);

        //3. Implement sorting to display books by price (both ascending and descending)
        // Ascending
        console.log("\n Book price in ascending order");
        const ascSort = await collection.find({}).sort({price: 1}).toArray();
        console.log(ascSort);

        // Descending
        console.log("\n Book price in descending order");
        const descSort = await collection.find({}).sort({price: -1}).toArray();
        console.log(descSort);

        //4. Use the limit and skip methods to implement pagination (5 books per page)
        console.log("\n page 2 through pagination");
        let page = 2;
        const pagesize=5;
        const pagination = await collection.find({}).skip((page-1)*pagesize).limit(pagesize).toArray();
        console.log(pagination);

        
                  //****Aggregation Pipeline

        //1. Create an aggregation pipeline to calculate the average price of books by genre
        console.log("\n Average price of books in every genre");
        const average = await collection.aggregate([
            {$group: {_id: "$genre", averagePrice: {$avg: "$price"}}}
        ]).toArray();
        console.log(average);

        //2. Create an aggregation pipeline to find the author with the most books in the collection
        console.log("\n Author with most books");
        const author = await collection.aggregate([
            {$group: {_id: "author", totalbooks: {$sum: 1}}},
            {$sort: {totalbooks: -1} },
            {$limit: 1}
        ]).toArray();
        console.log(author);

        //3. Implement a pipeline that groups books by publication decade and counts them
        console.log("\n Books by publication decade");
        const decade = await collection.aggregate([
            {$group:
                {_id: {$multiply: [ {$floor: {$divide: ["$published_year", 10]}}, 10]},
                total: {$sum: 1}
                 } 
            } 
        ]).toArray();
        console.log(decade);
        
                   //**** Indexing

        //1. Create an index on the title field for faster searches
        const titleIndex = await collection.createIndex({title: 1});

        //2. Create a compound index on author and published_year
         const compoundIndex = await collection.createIndex({author: 1, published_year: 1});

         //3. Use the explain() method to demonstrate the performance improvement with your indexes
         console.log("\n Performance improvement through title indexe");
         const indexStat = await collection.find({title: "Project Hail Mary"}).explain("executionStats");
         console.log("Execution time", indexStat.executionStats.executionTimeMillis);
         console.log("Docs examined", indexStat.executionStats.totalDocsExamined);
         console.log("Keys examined", indexStat.executionStats.totalKeysExamined);

        console.log("\n Performance improvement through compound indexes");
         const compoundStat = await collection.find({author: "Madeline Miller", published_year: {$gt: 2000
          }}).explain("executionStats");
        console.log("Execution time", compoundStat.executionStats.executionTimeMillis);
         console.log("Docs examined", compoundStat.executionStats.totalDocsExamined);
         console.log("Keys examined", compoundStat.executionStats.totalKeysExamined);

    } catch(err){
        console.error('Error occurred: ', err);
    } finally{
        // Close connection
        await client.close();
        console.log("Connection closed");

    }

}
queries().catch(console.error);