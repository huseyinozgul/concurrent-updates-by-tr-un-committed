const { Sequelize, DataTypes, Model } = require('sequelize');

const sequelize = new Sequelize('postgres', 'postgres', undefined,{
  host: 'localhost',
  dialect: 'postgres',
  logging: false,
  pool: {
    max: 150,
    min: 0,
    acquire: 30000,
    idle: 10000
  },
  schema: 'test'
});

class Product extends Model { }
class Order extends Model { }

Product.init({
  id: {
    type: DataTypes.INTEGER,
    autoIncrement: true,
    primaryKey: true
  },
  product: {
    type: DataTypes.STRING,
  },
  quantity: {
    type: DataTypes.INTEGER
  },
  createdAt: {
    type: DataTypes.DATE,
    defaultValue: Sequelize.NOW
  },  
  updatedAt: {
    type: DataTypes.DATE,
    defaultValue: Sequelize.NOW 
  }
}, {
  sequelize,
  modelName: 'product',
  freezeTableName: true
})

Order.init({
  id: {
    type: DataTypes.INTEGER,
    autoIncrement: true,
    primaryKey: true
  },
  product: {
    type: DataTypes.STRING,
  },
  order: {
    type: DataTypes.STRING
  },
  quantity: {
    type: DataTypes.INTEGER
  },
  createdAt: {
    type: DataTypes.DATE,
    defaultValue: Sequelize.NOW
  },
  updatedAt: {
    type: DataTypes.DATE,
    defaultValue: Sequelize.NOW
  }
}, {
  sequelize,
  modelName: 'order',
  freezeTableName: true
});

const createOrderWithTimeout = async (order) => {

  const transaction = await sequelize.transaction({
    isolationLevel: Sequelize.Transaction.ISOLATION_LEVELS.READ_UNCOMMITTED,
    autocommit: false
  });
  try {
    const product = await Product.findOne({ where: { product: order.product }, transaction });
    if (!product) {
      throw new Error('Product not found');
    }
    if ((product.quantity-order.quantity) < 0) {
      throw new Error(`Out of stock, ${order.product} requested ${order.quantity} but only ${product.quantity} available`);
    }
    await Product.update({ quantity: product.quantity - order.quantity }, { where: { product: order.product }, transaction });
    await Order.create({ order: order.order, product: order.product, quantity: order.quantity }, { transaction });
    await new Promise(resolve => setTimeout(resolve, 3000));
    await transaction.commit();
  } catch (error) {
    console.log('Error', error.message);
    await transaction.rollback();
  }
}

const generateDummyData = (count = 100) => {
  const data = [];
  for (let i = 0; i < count; i++) {
    data.push({
      isTimeout: i % 4 === 0,
      order: `Order ${i}`,
      product: i % 2 === 0 ? 'a' : 'b',
      quantity: Math.floor(Math.random() * 10)
    });
  }
  return data;
}

const main = async () => {
  await sequelize.sync({ force: true });

  await Product.create({ product: 'a', quantity: 200 });
  await Product.create({ product: 'b', quantity: 300 });

  const orders = generateDummyData();
  var promises = orders.map(async (product) => createOrderWithTimeout(product))

  await Promise.all(promises)
}



main()