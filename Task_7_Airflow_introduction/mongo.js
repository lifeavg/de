// Топ-5 известных комментариев
db.getSiblingDB("raitings")
  .getCollection("reviews")
  .find({})
  .sort({
    "at": -1
  })
  .limit(5)


// Все записи, где длина поля “content” составляет менее 5 символов
db.getSiblingDB("raitings")
  .getCollection("reviews")
  .find({
    "content": { $exists: true },
    $expr: {
      $lt: [
        { $strLenCP: '$content' },
        5
      ]
    }
  })

// Средний рейтинг по каждому дню (результат должен быть в виде timestamp type)
db.getSiblingDB("raitings")
  .getCollection("reviews")
  .aggregate([
    {
      $addFields: {
        "date": {
          $dateTrunc: {
            date: "$at",
            unit: "day"
          }
        },
      }
    },
    {
      $group: {
        _id: { "gdate": "$date" },
        "avg_score": { $avg: "$score" }
      }
    },
    {
      $project: { "date": { $toLong: "$_id.gdate" }, "avg_score": "$avg_score", "_id": 0 }
    }
  ])
