/*
 * Copyright (c) 2015, erukiti at gmail dot com
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
 * ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.erukiti.pasco1.common;

import java.util.Optional;
import java.util.function.Function;

public class Either<L, R> {
    private final Optional<L> left;
    private final Optional<R> right;

    public Either(Optional<L> left, Optional<R> right) {
        this.left = left;
        this.right = right;
    }

    static public <L, R> Either<L, R> createLeft(L left) {
        return new Either<>(Optional.of(left), Optional.empty());
    }

    static public <L, R> Either<L, R> createRight(R right) {
        return new Either<>(Optional.empty(), Optional.of(right));
    }

    public Optional<L> getLeft() {
        return left;
    }

    public Optional<R> getRight() {
        return right;
    }

    public <T> T match(Function<L, T> funcLeft, Function<R, T> funcRight) {
        if (left.isPresent()) {
            return funcLeft.apply(left.get());
        } else {
            return funcRight.apply(right.get());
        }
    }

}
