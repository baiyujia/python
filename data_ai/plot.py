import numpy as np
import matplotlib.pyplot as plt
import matplotlib as mpl
import seaborn as sns
x=np.random.normal(150, 100, 5000)


print(x.min(),x.max())
plt.figure()
plt.title('Member vs Casual')
plt.xlabel('Quarter')
plt.ylabel('Mean duration (min)')

#plt.hist(x,bins=100)
plt.plot(np.sort(x))
#plt.xticks(range(-100,500,100), [str(i) for i in range(-100,500,100)], rotation=45)
#sns.distplot(x,bins=100,hist=False)
plt.show()
