// Field documentation
const FIRST_NAME = {
  first_name: `First Name of the customer`
};

const LAST_NAME = {
  last_name: `Last Name of the customer`
};

const PHONE = {
  phone: `Phone number of the customer`
};

const EMAIL = {
  email: `Email address of the customer`
};

const WEBSITE = {
  website: `Website of the customer`
};

const cleaned_customer_data = {
  ...FIRST_NAME,
  ...LAST_NAME,
  ...PHONE,
  ...EMAIL,
  ...WEBSITE,
};
module.exports = {
  cleaned_customer_data
}