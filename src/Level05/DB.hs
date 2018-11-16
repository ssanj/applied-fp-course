{-# LANGUAGE OverloadedStrings #-}
module Level05.DB
  ( FirstAppDB (FirstAppDB)
  , initDB
  , closeDB
  , addCommentToTopic
  , getComments
  , getTopics
  , deleteTopic
  ) where

import           Control.Monad.IO.Class             (liftIO)

import           Data.Text                          (Text)
import qualified Data.Text                          as Text

import           Data.Bifunctor                     (first)
import           Data.Time                          (getCurrentTime)

import           Database.SQLite.Simple             (Connection,
                                                     Query (fromQuery))
import qualified Database.SQLite.Simple             as Sql

import qualified Database.SQLite.SimpleErrors       as Sql
import           Database.SQLite.SimpleErrors.Types (SQLiteResponse)

import           Level05.Types                      (Comment, CommentText,
                                                     Error (DBError), Topic,
                                                     fromDBComment,
                                                     getCommentText, getTopic,
                                                     mkTopic)

import           Level05.AppM                       (AppM(..))

-- We have a data type to simplify passing around the information we need to run
-- our database queries. This also allows things to change over time without
-- having to rewrite all of the functions that need to interact with DB related
-- things in different ways.
newtype FirstAppDB = FirstAppDB
  { dbConn  :: Connection
  }

-- Quick helper to pull the connection and close it down.
closeDB
  :: FirstAppDB
  -> IO ()
closeDB =
  Sql.close . dbConn

initDB
  :: FilePath
  -> IO ( Either SQLiteResponse FirstAppDB )
initDB fp = Sql.runDBAction $ do
  -- Initialise the connection to the DB...
  -- - What could go wrong here?
  -- - What haven't we be told in the types?
  con <- Sql.open fp
  -- Initialise our one table, if it's not there already
  _ <- Sql.execute_ con createTableQ
  pure $ FirstAppDB con
  where
  -- Query has an `IsString` instance so string literals like this can be
  -- converted into a `Query` type when the `OverloadedStrings` language
  -- extension is enabled.
    createTableQ =
      "CREATE TABLE IF NOT EXISTS comments (id INTEGER PRIMARY KEY, topic TEXT, comment TEXT, time INTEGER)"

runDB
  :: (a -> Either Error b)
  -> IO a
  -> AppM b
runDB f ioa = AppM $ do
   esa <- Sql.runDBAction ioa
   return $ case esa of
    Left sqlEr -> Left $ DBError sqlEr -- map errors to Either Error b
    Right a -> f a -- map successes to Either Error b

getComments
  :: FirstAppDB
  -> Topic
  -> AppM [Comment]
getComments fdb topic =
    let
    sql = "SELECT id,topic,comment,time FROM comments WHERE topic = ?"
  -- There are several possible implementations of this function. Particularly
  -- there may be a trade-off between deciding to throw an Error if a DBComment
  -- cannot be converted to a Comment, or simply ignoring any DBComment that is
  -- not valid.
  in runDB (traverse fromDBComment) $ Sql.query (dbConn fdb) sql (Sql.Only $ (getTopic topic :: Text))

addCommentToTopic
  :: FirstAppDB
  -> Topic
  -> CommentText
  -> AppM ()
addCommentToTopic fdb topic comment =
  let
    sql = "INSERT INTO comments (topic,comment,time) VALUES (?,?,?)"
  in do ct <- liftIO getCurrentTime
        runDB Right $ Sql.execute (dbConn fdb) sql (getTopic topic, getCommentText comment, ct)

getTopics
  :: FirstAppDB
  -> AppM [Topic]
getTopics fdb =
  let
    sql = "SELECT DISTINCT topic FROM comments"
  in
    runDB (traverse (mkTopic . Sql.fromOnly)) $ Sql.query_ (dbConn fdb) sql

deleteTopic
  :: FirstAppDB
  -> Topic
  -> AppM ()
deleteTopic fdb topic =
  let
    sql = "DELETE FROM comments WHERE topic = ?"
  in
    runDB Right $ Sql.execute (dbConn fdb) sql $ Sql.Only (getTopic topic :: Text)
