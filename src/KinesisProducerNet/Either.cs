using System;

namespace KinesisProducerNet
{
    public class Either<TL, TR>
    {
        private readonly TL left;
        private readonly TR right;
        private readonly bool isLeft;

        protected Either(TL left)
        {
            this.left = left;
            this.isLeft = true;
        }

        protected Either(TR right)
        {
            this.right = right;
            this.isLeft = false;
        }

        private T Match<T>(Func<TL, T> leftFunc, Func<TR, T> rightFunc)
        {
            if (leftFunc == null)
            {
                throw new ArgumentNullException(nameof(leftFunc));
            }

            if (rightFunc == null)
            {
                throw new ArgumentNullException(nameof(rightFunc));
            }

            return this.isLeft ? leftFunc(this.left) : rightFunc(this.right);
        }

        public TL LeftOrDefault => this.Match(l => l, r => default(TL));

        public TR RightOrDefault => this.Match(l => default(TR), r => r);

        public static implicit operator Either<TL, TR>(TL left) => new Either<TL, TR>(left);

        public static implicit operator Either<TL, TR>(TR right) => new Either<TL, TR>(right);
    }
}