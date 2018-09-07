package trabalhoFinal

case class Reddit(author: String, body: String, author_flair_text: String, gilded: String, score: Long,
                  link_id: String, retrieved_on: java.sql.Timestamp, author_flair_css_class: String, subreddit: String,
                  edited: Boolean, ups: Long, controversiality: Long, created_utc: Long,
                  parent_id: String, subreddit_id: String, id: String, distinguished: String, sentiment:Float)
