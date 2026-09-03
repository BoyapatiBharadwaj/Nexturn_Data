case class Product(
  productId: String,
  productName: String,
  category: String,
  reorderLevel: Int,
  initialStock: Int
)

case class PharmacyEvent(
  eventId: String,
  timestamp: String,
  productId: String,
  quantity: Int,
  eventType: String
)

case class StockBalance(
  productId: String,
  productName: String,
  category: String,
  initialStock: Int,
  totalSold: Int,
  totalRestocked: Int,
  currentStock: Int,
  reorderLevel: Int,
  stockStatus: String
)
