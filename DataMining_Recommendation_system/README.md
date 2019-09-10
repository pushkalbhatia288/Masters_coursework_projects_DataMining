# Data-mining-recommendation-system
Handling missing values: Implementation of my recommendation system is done using Item-based
approach. Also I used Pearson Coefficient to find similarities.
The approach basically used was Mean value Imputation.
To handle missing items in my recommendation system, I have taken the average rating of all the other
items for the user with whom the item to be predicted appeared. In other words, I have calculated the
average rating of all the other items, that the user rated, and assigned that rating for the missing item.
This rating was added to the training data, so for the next time, if the same item comes, it has some
prediction this time. (Item → Average rating of all the other items for the assigned user).
If a user is missing, but the item is found (checked by a flag value), I have taken the average rating for
that value, by other users.
In case, the user and item both are missing, for that I simply took the predicted rating as 2.5, so that
the absolute difference can’t be more than 2.5 in general case, and we get minimum errors for such
cases, with the prediction still being close.
Handling outlier values: In case the predicted values were less than 0 or greater than 5, I updated the
predicted rating for that particular item as the average rating of that item. In other words, this time I
calculated the average rating of this item, rated by all the users. (Item → average rating of the item by
users.)
This way, I predicted all the values of the testing data and got the predicted ratings, beating the baseline
and not getting any NAN or missing values.
Also, for the first task in which Mlib was used, same thing was done to handle missing values i.e. using
the same technique described above.
